package main

// DAM Client Cache Manager
// May 2020: JB
//
//

import (
	"archive/zip"
	"fmt"
	"io"
	"os"

	"bufio"
	"bytes"

	//"net/url"
	"strconv"
	"context"
	"database/sql"

	//	"io/ioutil"

	"log"
	"net/http"

	"os/exec"
	"path/filepath"
	"strings"
	"time"

	//	"github.com/hashicorp/go-retryablehttp" // http client lib
	"github.com/Tkanos/gonfig" // config management support
	//	"github.com/beevik/etree"  // xml lib
	"github.com/lib/pq" // golang postgres db driver
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
	DebugLogging	  string
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
		println( message )
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
		printMessage("[DAL] ERROR not connected to DB!")

		panic(err)

	}
	err = db.Ping()
	if err != nil {

		printMessage("[DAL] ERROR not connected [ping] to DB!")
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
			//return nil;
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
	//    files := []string{"testdir/2/employee.xml", "testdir/1/Drinks Menu.oet"}
	uid := nowAsUnixMilli()
	output := fmt.Sprintf( "%s/%s/downloads/%d-precache.zip", sessionConfig.ChangesetPath, ticketdir, uid)


	//output :=  sessionConfig.ChangesetPath + "/" + ticketdir + "/downloads/" + "precache-" + ticketdir + ".zip"

	if err := zipFiles(output, files); err != nil {
//		panic(err)
		logMessage( "zipFiles(): " + err.Error(), ticketdir, "ERROR")
	}
	fmt.Println("Zipped File:", output)
	return output
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
	filename :=fmt.Sprintf( "%s/%s/downloads/%d-postcache.zip", sessionConfig.ChangesetPath, ticket, uid)
	file, err := os.Create( filename )

	if err != nil {
		logMessage(err.Error(), ticket, "ERROR")
	}
	n, err := io.Copy(file, r.Body)
	if err != nil {
		logMessage("[DCC] uploadHandler(): " + err.Error(), ticket, "ERROR")
	}

	w.Write([]byte(fmt.Sprintf("%d bytes are recieved.\n", n)))

	//_, err = Unzip( filename, sessionConfig.ChangesetPath + "/" + ticket + "/")

	stdout, stderr := unzipWrapper( filename,sessionConfig.ChangesetPath + "/" + ticket + "/")

	println( "stdout " + stdout )
	println( "stderr " + stderr )	

	if( err != nil) {
		logMessage(err.Error(), ticket, "ERROR")
	}

}


// wrapper around linux unzip utility
func unzipWrapper(zipfile string, outputdir string) (string, string) {
	println( "unzipWrapper : " + zipfile + ", " + outputdir)

	cmd := exec.Command("unzip", "-o", zipfile, "-d", outputdir )

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

			w.Write( []byte(sessionConfig.CachingEnabled ) )
			return
		}

		if strings.Contains(r.URL.Path, "createArchive") {
			params := strings.Split(r.RequestURI, ",")

			if len(params) < 1 {
				return
			}

			ticket := params[1]
			buildAndSendArchive(  w,r, ticket)

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
	case "POST": 
		if strings.Contains(r.URL.Path, "/upload") {
			uploadHandler( w, r)
		}
	}
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

// func main() {

//     // List of Files to Zip
//     files := []string{"example.csv", "data.csv"}
//     output := "done.zip"

//     if err := ZipFiles(output, files); err != nil {
//         panic(err)
//     }
//     fmt.Println("Zipped File:", output)
// }







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
	relfilename, _ := filepath.Rel( sessionConfig.ChangesetPath, filename)


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
