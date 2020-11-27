package main

// DAM Client Cache Manager
// May 2020: JB
//

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Tkanos/gonfig" // config management support
	"github.com/lib/pq"        // golang postgres db driver
)

//-----------------------------------------------------------------------------------------------//-----------------------------------------------------------------------------------------------
// globals
//-----------------------------------------------------------------------------------------------//-----------------------------------------------------------------------------------------------

var db *sql.DB                      // db connection
var sessionConfig = configuration{} // runtime config
var gBuild string
var gConfigPath string
var gMirror string

// holds the config, populated from config.json
type configuration struct {
	DBhost            string
	DBusr             string
	DBpw              string
	DBPort            string
	ListenPort        string
	DBName            string
	WorkingFolderPath string
	ChangesetPath     string
	MirrorCkmPath     string
	CachingEnabled    string
	DebugLogging      string
}

func printMessage(a ...interface{}) {

	if strings.ToUpper(sessionConfig.DebugLogging) == "YES" {
		fmt.Println(a...)
	}

}
func findFilename(pattern string, dir string) string {
	cmd := exec.Command("find", dir, "-name ", pattern)

	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf
	cmd.Run()

	stdout := outbuf.String()
	return stdout
}

func grepDir(pattern string, dir string) string {
	cmd := exec.Command("grep", "-r", "--exclude-dir=downloads", pattern, dir)

	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf
	cmd.Run()

	stdout := outbuf.String()
	return stdout
}

func readLn(r *bufio.Reader) (string, error) {
	var (
		isPrefix = true
		err      error
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}
func readlines2(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	r := bufio.NewReader(file)
	s, e := readLn(r)
	for e == nil {

		lines = append(lines, s)
		s, e = readLn(r)
	}

	return lines, nil
}
func readFile(theFile *os.File) ([]string, error) {

	var lines []string
	r := bufio.NewReader(theFile)
	s, e := readLn(r)
	for e == nil {

		lines = append(lines, s)
		s, e = readLn(r)
	}

	return lines, nil
}

func logMessage(message, ticket, logtype string) error {

	println(message + " [ " + logtype + " ]")

	err := db.Ping()
	if err != nil {
		return err
	}

	if logtype == "ERROR " {
		//logFileMessage(message, ticket, logtype)
		println(message)
	}

	sqlStatement := `
	INSERT INTO public.log
	(message, messagetime, fromcomponent, focusticket, logtype)
	VALUES( $1, $2, $3, $4, $5);`
	_, err = db.Exec(sqlStatement, message, time.Now(), "DAMClientCache v"+gBuild, ticket, logtype)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			printMessage("[DCC] pq ERROR:", err.Code.Name())
			logMessage("logMessage() couldn't INSERT into log :"+err.Code.Name(), "", "ERROR")
		}
	}
	return err
}

// initializes the db [postgres] connection with params held in the config file.
func initDb() {

	var err error
	var _ pq.NullTime

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", sessionConfig.DBhost, sessionConfig.DBPort, sessionConfig.DBusr, sessionConfig.DBpw, sessionConfig.DBName)

	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		printMessage("[DCC] ERROR not connected to DB!")

		panic(err)

	}
	err = db.Ping()
	if err != nil {

		printMessage("[DCC] ERROR not connected [ping] to DB!")
		panic(err)

	}

}

// wrapper around linux grep utility
func grepFile(file string, pattern string) string {

	cmd := exec.Command("grep", pattern, file)

	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf

	err := cmd.Run()
	if err != nil {
		logMessage("grepFile finished with error: "+err.Error(), "", "ERROR")
	}
	stdout := outbuf.String()
	return stdout
}

func defaultRetryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	// do not retry on context.Canceled or context.DeadlineExceeded
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	if err != nil {
		return true, err
	}
	// Check the response code. We retry on 500-range responses to allow
	// the server time to recover, as 500's are typically not permanent
	// errors and may relate to outages on the server side. This will catch
	// invalid response codes as well, like 0 and 999.
	if resp.StatusCode == 0 || (resp.StatusCode >= 405 && resp.StatusCode != 501 && resp.StatusCode != 424) {
		return true, nil
	}

	return false, nil
}

func main() {

	configFilename := "config.json"

	if len(os.Args) > 1 {
		aSwitch := os.Args[1]
		if strings.ToLower(aSwitch) == "-v" {
			println("DAMClientCache v" + gBuild)
			return
		}
	}

	printMessage("[DCC] " + gBuild)
	exePath := os.Args[0]
	gConfigPath, _ := filepath.Split(exePath)

	err := gonfig.GetConf(gConfigPath+configFilename, &sessionConfig)
	if err != nil {
		printMessage("[DCC] ERROR getting config: " + err.Error())
		panic(err)
	}
	initDb()
	defer db.Close()

	if len(os.Args) > 1 {
		aSwitch := os.Args[1]
		if strings.ToLower(aSwitch) == "bulkmap" {

			return
		}
	}

	http.HandleFunc("/", handler)
	initDb()
	defer db.Close()

	log.Println("Listening... (" + sessionConfig.ListenPort + ")")

	err = http.ListenAndServe(":"+sessionConfig.ListenPort, nil)
	if err != nil {
		fmt.Println("ERROR " + err.Error())
	}
}
func nowAsUnixMilli() int64 {
	return time.Now().UnixNano() / 1e6
}
func createArchive(ticketdir string) string {
	// List of Files to Zip
	var files []string

	root := sessionConfig.ChangesetPath + "/" + ticketdir
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {

		if strings.Contains(path, "downloads") {
			return filepath.SkipDir
		}

		if !info.IsDir() {
			files = append(files, path)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	uid := nowAsUnixMilli()
	output := fmt.Sprintf("%s/%s/downloads/%d-precache.zip", sessionConfig.ChangesetPath, ticketdir, uid)

	if err := zipFiles(output, files); err != nil {
		logMessage("zipFiles(): "+err.Error(), ticketdir, "ERROR")
	}
	fmt.Println("Zipped File:", output)
	return output
}

func linkTicketHandler(w http.ResponseWriter, r *http.Request) {

	theTicket := r.FormValue("theTicket")
	theDescription  := r.FormValue("theDescription")
	theLead := r.FormValue("theLead")
	theAssignee := r.FormValue("theAssignee")

	theFolder := ""

	sql := `select folder from damfolder where jirakey = ''`
	rows, err := db.Query(sql, )
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			printMessage("[DCC] pq ERROR:", err.Code.Name())
			logMessage("linkTicketHandler() couldn't SELECT from damfolder :"+err.Code.Name(), "", "ERROR")
			http.Error(w, "linkTicketHandler() couldn't SELECT from damfolder :"+err.Code.Name(), http.StatusNotModified)			
		}
		return 
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan( // just get the first row
			&theFolder,
		)
		break
	}

	sqlStatement := `
		update change damfolder set jirakey = $1 where folder = $2`

	_, err = db.Exec(sqlStatement, theTicket, theFolder)
	if err, ok := err.(*pq.Error); ok {
		printMessage("[DCC] pq ERROR:", err.Code.Name())
		logMessage("linkTicketHandler() couldn't UPDATE damfolder :"+err.Code.Name(), theFolder, "ERROR")
		http.Error(w, "linkTicketHandler() couldn't UPDATE damfolder :"+err.Code.Name(), http.StatusNotModified)
		return
	}
	
	sqlStatement = `
	INSERT INTO public."change"
        (jirakey, folder, description, "lead", jiraassignee, targetstartdate, targetrepositoryenddate, targetenddate, percentagecomplete, active, implementationnotes, state_ready, uploading)
	VALUES($1, $2, $3, $4, $5, null, null, null, '', true, '', false, false);`

	_, err = db.Exec(sqlStatement, theTicket, theFolder, theDescription, theLead, theAssignee);
	if err, ok := err.(*pq.Error); ok {
		printMessage("[DCC] pq ERROR:", err.Code.Name())
		logMessage("linkTicketHandler() couldn't UPDATE change :"+err.Code.Name(), theFolder, "ERROR")
		http.Error(w, "linkTicketHandler() couldn't UPDATE change :"+err.Code.Name(), http.StatusNotModified)
		return
	}
	logMessage("linkTicketHandler() : returning folder : " + theFolder, "", "INFO")
	w.Write([]byte( theFolder) );
}

func readyHandler(w http.ResponseWriter, r *http.Request) {

	theState := r.FormValue("theState")
	theFolder := r.FormValue("theFolder")

	bReady := true

	if theState != "ready" {
		bReady = false
	}

	sqlStatement := `
		update "change" ch 
		set state_ready = $1 
		from damfolder df
		where df.jirakey = ch.jirakey
		and df.folder = $2`

	_, err := db.Exec(sqlStatement, bReady, theFolder)
	if err, ok := err.(*pq.Error); ok {
		printMessage("[DCC] pq ERROR:", err.Code.Name())
		logMessage("readyHandler() couldn't UPDATE change :"+err.Code.Name(), theFolder, "ERROR")
		http.Error(w, "readyHandler() couldn't UPDATE change :"+err.Code.Name(), http.StatusNotModified)
		return
	}

}

func wipRemoveHandler(w http.ResponseWriter, r *http.Request) {
	theFolder := r.FormValue("theFolder")
	theTemplateID := r.FormValue("theTemplateID")
	fullfilepath := ""

	//theFilePath := theFolder + "\\" + theTemplateName
	//theHash := ""

	sql := `select fullfilepath from damasset WHERE folder = $1 AND resourcemainid = $2`
	rows, err := db.Query(sql, theFolder, theTemplateID )
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			printMessage("wipRemoveHandler() ERROR:", err.Code.Name())
			logMessage("wipRemoveHandler() couldn't SELECT from damasset :"+err.Code.Name(), "", "ERROR")
		}
		return 
	}
	
	defer rows.Close()
		for rows.Next() {
			err = rows.Scan(
			&fullfilepath,
		)
	}

	fullfilepath = strings.ReplaceAll(fullfilepath, "\\", "/")

	logMessage( "wipRemoveHandler trying to remove " + fullfilepath, theFolder, "DEBUG" )
	os.Remove( fullfilepath )
	

	sqlStatement := `
		DELETE FROM public.damasset WHERE folder = $1 AND resourcemainid = $2`

	_, err = db.Exec(sqlStatement, theFolder, theTemplateID)
	if err, ok := err.(*pq.Error); ok {
		printMessage("[DCC] pq ERROR:", err.Code.Name())
		logMessage("wipRemoveHandler() couldn't DELETE from damasset :"+err.Code.Name(), theFolder, "ERROR")
		http.Error(w, "wipRemoveHandler() couldn't DELETE from damasset :"+err.Code.Name(), http.StatusNotModified)
		return
	}

}

func wipHandler(w http.ResponseWriter, r *http.Request) {
	// get the ticket directory from the request

	theFolder := r.FormValue("theFolder")
	theTemplateID := r.FormValue("theTemplateID")
	theTemplateName := r.FormValue("theTemplateName")
	theFilePath := sessionConfig.ChangesetPath + "\\" + theFolder + "\\" + theTemplateName

	theHash := ""

	sqlStatement := `
		INSERT INTO public.damasset
		(      fullfilepath, filename, resourcemainid, initialmd5, initialversion, modified, islatest,  created, updated, importanttouser, folder)
		VALUES(   $2,            $3,              $4,            $5,         0,        $8,     true,      $6,      $7,      $9,              $1);	`

	_, err := db.Exec(sqlStatement, theFolder, theFilePath, theTemplateName, theTemplateID, theHash, time.Now(), time.Now(), false, 1)
	if err, ok := err.(*pq.Error); ok {
		printMessage("[DCC] pq ERROR:", err.Code.Name())
		logMessage("WIPHandler() couldn't INSERT into damasset :"+err.Code.Name(), theFolder, "ERROR")
		http.Error(w, "WIPHandler() couldn't INSERT into damasset :"+err.Code.Name(), http.StatusNotModified)
		return
	}

}

func uploadHandler(w http.ResponseWriter, r *http.Request) {

	// get the ticket directory from the request

	params := strings.Split(r.RequestURI, ",")

	if len(params) < 1 {
		return
	}

	ticket := params[1]
	uid := nowAsUnixMilli()
	//filename := fmt.Sprintf( "./%s-%d.zip", ticket, uid)
	filename := fmt.Sprintf("%s/%s/downloads/%d-postcache.zip", sessionConfig.ChangesetPath, ticket, uid)
	file, err := os.Create(filename)

	if err != nil {
		logMessage(err.Error(), ticket, "ERROR")
	}
	n, err := io.Copy(file, r.Body)
	if err != nil {
		logMessage("[DCC] uploadHandler(): "+err.Error(), ticket, "ERROR")
	}

	w.Write([]byte(fmt.Sprintf("%d bytes are recieved.\n", n)))

	//_, err = Unzip( filename, sessionConfig.ChangesetPath + "/" + ticket + "/")

	stdout, stderr := unzipWrapper(filename, sessionConfig.ChangesetPath+"/"+ticket+"/")

	println("stdout " + stdout)
	println("stderr " + stderr)

	if err != nil {
		logMessage(err.Error(), ticket, "ERROR")
	}

}

func getTemplateID(filepath string) string {

	templateID := ""
	file, err := os.Open(filepath)
	if err != nil {
		logMessage("getTemplateID os.Open() failed : "+err.Error(), "", "ERROR")
		return templateID
	}

	lines, err := readFile(file)
	if err != nil {
		logMessage("getTemplateID readFile() failed : "+err.Error(), "", "ERROR")
		return templateID
	}
	content := strings.Join(lines, " ")
	splits := strings.Split(strings.ToLower(content), "<id>")
	if len(splits) > 1 {
		templateID = (splits[1])[0:36] // TODO: HACK: assumes id format is fixed....
	}
	defer file.Close()

	return templateID

}

// wrapper around linux unzip utility
func unzipWrapper(zipfile string, outputdir string) (string, string) {
	println("unzipWrapper : " + zipfile + ", " + outputdir)

	syscall.Umask(0002)

	cmd := exec.Command("unzip", "-o", zipfile, "-d", outputdir)

	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf
	cmd.Run()

	stdout := outbuf.String()
	stderr := errbuf.String()

	return stdout, stderr
}

func handler(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "GET":
		if strings.Contains(r.URL.Path, "isCachingEnabled") {

			w.Write([]byte(sessionConfig.CachingEnabled))
			return
		}

		if strings.Contains(r.URL.Path, "createArchive") {
			params := strings.Split(r.RequestURI, ",")

			if len(params) < 1 {
				return
			}

			ticket := params[1]
			buildAndSendArchive(w, r, ticket)

		}

		if strings.Contains(r.URL.Path, "transform_support") {

			buildAndSendSupport(w, r)
		}

		if strings.Contains(r.URL.Path, "remove") {
			params := strings.Split(r.RequestURI, ",")

			if len(params) < 1 {
				return
			}

			//fileToParse, err := url.QueryUnescape(params[1])
			// if err != nil {
			// 	logMessage("Failed to decode parameter", "", "ERROR")
			// } else {

			// }
		}

		if strings.Contains(r.URL.Path, "templatebyid") {
			params := strings.Split(r.RequestURI, ",")

			if len(params) < 1 {
				return
			}
			var sTemplateID = params[1]
			queryTemplateByID(w, sTemplateID)
		}


		if strings.Contains(r.URL.Path, "change_status") {
			params := strings.Split(r.RequestURI, ",")

			if len(params) < 1 {
				return
			}
			var sTemplateID = params[1]
			queryChangeStatus(w, sTemplateID)
		}


	case "POST":
		if err := r.ParseForm(); err != nil {
			fmt.Fprintf(w, "ParseForm() err: %v", err)
			return
		}

		if strings.Contains(r.URL.Path, "/upload") {
			uploadHandler(w, r)
		}

		if strings.Contains(r.URL.Path, "/RemoveWIP") {
			wipRemoveHandler(w, r)
		}

		if strings.Contains(r.URL.Path, "/WIP") {
			wipHandler(w, r)
		}

		if strings.Contains(r.URL.Path, "/ready") {
			readyHandler(w, r)
		}

		if strings.Contains(r.URL.Path, "/linkTicket") {
			linkTicketHandler(w, r)
		}

		

	}
}


func queryChangeStatus(w http.ResponseWriter, sTemplateID string) {


	var active = false
	var uploading = false
	var stateReady = false

	sql := `select active, state_ready, uploading from change where jirakey = $1`
	rows, err := db.Query(sql, sTemplateID)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			printMessage("[DCC] pq ERROR:", err.Code.Name())
			logMessage("queryChangeStatus() couldn't SELECT from mirrorstate :"+err.Code.Name(), "", "ERROR")
		}
		return
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(
			&active,
			&stateReady,
			&uploading,
		)
	}

	var json = fmt.Sprintf(`{"active": %t,"uploading": %t,"ready" : %t}`, active, uploading, stateReady )	

	logMessage("queryChangeStatus() returning json : "+json, "", "INFO")

	w.Write([]byte(json))

}

func queryTemplateByID(w http.ResponseWriter, sTemplateID string) {

	var sfilepath = ""

	sql := `select filepath from mirrorstate where templateid = $1`
	rows, err := db.Query(sql, sTemplateID)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			printMessage("[DCC] pq ERROR:", err.Code.Name())
			logMessage("queryTemplateByID() couldn't SELECT from mirrorstate :"+err.Code.Name(), "", "ERROR")
		}
		return
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(
			&sfilepath,
		)
	}

	w.Write([]byte(sfilepath))
}

func buildAndSendSupport(w http.ResponseWriter, r *http.Request) {

	Filename := "/opt/ckm-mirror/transform-support.zip"

	//Check if file exists and open
	Openfile, err := os.Open(Filename)
	defer Openfile.Close() //Close after function return
	if err != nil {
		//File not found, send 404
		http.Error(w, "File not found.", 404)
		return
	}

	//File is found, create and send the correct headers

	//Get the Content-Type of the file
	//Create a buffer to store the header of the file in
	FileHeader := make([]byte, 512)
	//Copy the headers into the FileHeader buffer
	Openfile.Read(FileHeader)
	//Get content type of file
	FileContentType := http.DetectContentType(FileHeader)

	//Get the file size
	FileStat, _ := Openfile.Stat()                     //Get info from file
	FileSize := strconv.FormatInt(FileStat.Size(), 10) //Get file size as a string

	//Send the headers
	w.Header().Set("Content-Disposition", "attachment; filename="+Filename)
	w.Header().Set("Content-Type", FileContentType)
	w.Header().Set("Content-Length", FileSize)

	//Send the file
	//We read 512 bytes from the file already, so we reset the offset back to 0
	Openfile.Seek(0, 0)
	io.Copy(w, Openfile) //'Copy' the file to the client

}

func buildAndSendArchive(w http.ResponseWriter, r *http.Request, ticket string) {
	Filename := createArchive(ticket)

	//Check if file exists and open
	Openfile, err := os.Open(Filename)
	defer Openfile.Close() //Close after function return
	if err != nil {
		//File not found, send 404
		http.Error(w, "File not found.", 404)
		return
	}

	//File is found, create and send the correct headers

	//Get the Content-Type of the file
	//Create a buffer to store the header of the file in
	FileHeader := make([]byte, 512)
	//Copy the headers into the FileHeader buffer
	Openfile.Read(FileHeader)
	//Get content type of file
	FileContentType := http.DetectContentType(FileHeader)

	//Get the file size
	FileStat, _ := Openfile.Stat()                     //Get info from file
	FileSize := strconv.FormatInt(FileStat.Size(), 10) //Get file size as a string

	//Send the headers
	w.Header().Set("Content-Disposition", "attachment; filename="+Filename)
	w.Header().Set("Content-Type", FileContentType)
	w.Header().Set("Content-Length", FileSize)

	//Send the file
	//We read 512 bytes from the file already, so we reset the offset back to 0
	Openfile.Seek(0, 0)
	io.Copy(w, Openfile) //'Copy' the file to the client
}

// Unzip will decompress a zip archive, moving all files and folders
// within the zip file (parameter 1) to an output directory (parameter 2).
func Unzip(src string, dest string) ([]string, error) {

	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, err
	}
	defer r.Close()

	for _, f := range r.File {

		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)

		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return filenames, fmt.Errorf("%s: illegal file path", fpath)
		}

		filenames = append(filenames, fpath)

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return filenames, err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return filenames, err
		}

		rc, err := f.Open()
		if err != nil {
			return filenames, err
		}

		_, err = io.Copy(outFile, rc)

		// Close the file without defer to close before next iteration of loop
		outFile.Close()
		rc.Close()

		if err != nil {
			return filenames, err
		}
	}
	return filenames, nil
}

// ZipFiles compresses one or many files into a single zip archive file.
// Param 1: filename is the output zip file's name.
// Param 2: files is a list of files to add to the zip.
func zipFiles(filename string, files []string) error {

	newZipFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer newZipFile.Close()

	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	// Add files to zip
	for _, file := range files {
		if err = addFileToZip(zipWriter, file); err != nil {
			return err
		}
	}
	return nil
}

func addFileToZip(zipWriter *zip.Writer, filename string) error {

	fileToZip, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	// Get the file information
	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	relfilename, _ := filepath.Rel(sessionConfig.ChangesetPath, filename)

	//header.Name = filename
	header.Name = relfilename

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}
