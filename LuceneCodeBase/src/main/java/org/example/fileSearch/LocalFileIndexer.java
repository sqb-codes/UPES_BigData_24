package org.example.fileSearch;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LocalFileIndexer {

    private static void indexFile(File file, IndexWriter writer) throws IOException {
        Document doc = new Document();

        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder content = new StringBuilder();
        String line;

        while((line = reader.readLine()) != null) {
            content.append(line).append("\n");
        }
        reader.close();

        doc.add(new TextField("path", file.getPath(), TextField.Store.YES));
        doc.add(new TextField("content", content.toString(), TextField.Store.YES));
        writer.addDocument(doc);
    }

    public static void main(String[] args) throws IOException, ParseException {
        String filesPath = "src/main/resources/text_files";
        String indexPath = "src/main/resources/index";

        /* Step-1 : Index local files */
        StandardAnalyzer analyzer = new StandardAnalyzer();
        Directory indexDirectory = FSDirectory.open(Paths.get(indexPath));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(indexDirectory, config);

        File[] files = new File(filesPath).listFiles();
        if(files != null) {
            for(File file : files) {
                System.out.println(file.getName());
                if(file.isFile() && file.getName().endsWith(".txt")) {
                    indexFile(file, writer);
                }
            }
        }
        writer.close();


        /* Step-2 : Perform search on indexed files */
        DirectoryReader reader = DirectoryReader.open(indexDirectory);
        IndexSearcher searcher = new IndexSearcher(reader);
        String queryString = "genetic";
        QueryParser parser = new QueryParser("content", analyzer);
        Query query = parser.parse(queryString);

        TopDocs results = searcher.search(query, 5);
        System.out.println("Total hits : " + results.totalHits.value);

        for(ScoreDoc score : results.scoreDocs) {
            Document doc = searcher.doc(score.doc);
            System.out.println("File : " + doc.get("path"));
            System.out.println("Content : " + doc.get("content"));
        }

        reader.close();

    }
}
