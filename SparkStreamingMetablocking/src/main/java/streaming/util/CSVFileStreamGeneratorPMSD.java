package streaming.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;

import DataStructures.Attribute;
import DataStructures.EntityProfile;

/**
 * A utility for creating a sequence of files of integers in the file system so
 * that Spark can treat them like a stream. This follows a standard pattern to
 * ensure correctness: each file is first created in another folder and then
 * atomically renamed into the destination folder so that the file's point of
 * creation is unambiguous, and is correctly recognized by the streaming
 * mechanism.
 *
 * Each generated file has the same number of key/value pairs, where the keys
 * have the same names from file to file, and the values are random numbers, and
 * thus vary from file to file.
 *
 * This class is used by several of the streaming examples.
 */
public class CSVFileStreamGeneratorPMSD {

	private File _root;
	private File _prep;
	private File _dest;
	private int _nFiles;
	private int _nRecords;
	private int _betweenFilesMsec;
	private String _fileInput;
	private Random _random = new Random();
	private ArrayList<EntityProfile> EntityList;

	public CSVFileStreamGeneratorPMSD(String fileInput, int nFiles, int nRecords, int betweenFilesMsec) {
		_nFiles = nFiles;
		_nRecords = nRecords;
		_betweenFilesMsec = betweenFilesMsec;
		_fileInput = fileInput;

		// reading the files
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(new FileInputStream(fileInput));
			EntityList = (ArrayList<EntityProfile>) ois.readObject();
			ois.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// end reading

		_root = new File(File.separator + "tmp" + File.separator + "streamFiles");
		makeExist(_root);

		_prep = new File(_root.getAbsolutePath() + File.separator + "prep");
		makeExist(_prep);

		_dest = new File(_root.getAbsoluteFile() + File.separator + "dest");
		makeExist(_dest);
	}

	public File getDestination() {
		return _dest;
	}

	// fill a file with integers
	private void writeOutput(File f) throws FileNotFoundException {
		PrintWriter p = new java.io.PrintWriter(f);
		try {
			for (int i = 1; i <= _nRecords; i++) {
				// StreamingEntityPMSD item = new
				// StreamingEntityPMSD(_fileInput, _random, "Key_" + i);
				int whichCat = _random.nextInt(EntityList.size()-1);
				EntityProfile entityStreaming = EntityList.get(whichCat);
				String csvFormat = i + ";";
				csvFormat+=entityStreaming.getEntityUrl()+";";
				for (Attribute entityAtt : entityStreaming.getAttributes()) {
					csvFormat+=entityAtt.getValue()+";";
				}
				p.println(csvFormat);
			}
		} finally {
			p.close();
		}
	}

	private static void makeExist(File dir) {
		dir.mkdir();
	}

	// make the sequence of files by creating them in one place and renaming
	// them into the directory where Spark is looking for them
	// (file-based streaming requires "atomic" creation of the files)
	public void makeFiles() throws IOException, InterruptedException {
		for (int n = 1; n <= _nFiles; n++) {
			File f = File.createTempFile("Spark_", ".txt", _prep);
			writeOutput(f);
			File nf = new File(_dest + File.separator + f.getName());
			f.renameTo(nf);
			nf.deleteOnExit();
			Thread.sleep(_betweenFilesMsec);
		}
	}

}
