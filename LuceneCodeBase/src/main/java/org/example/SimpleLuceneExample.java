package org.example;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.IOException;

public class SimpleLuceneExample {

    public static void addDocument(IndexWriter writer, String title, String content) throws IOException {
        Document doc = new Document();
        doc.add(new TextField("title", title, TextField.Store.YES));
        doc.add(new TextField("content", content, TextField.Store.YES));
        writer.addDocument(doc);
    }

    public static void main(String[] args) throws IOException, ParseException {
        // 1. Create a StandardAnalyzer Object
        // tokenize the text into words and remove stopwords
        StandardAnalyzer analyzer = new StandardAnalyzer();

        /*
        * 2. Create an in-memory index (RAMDirectory (deprecated))
        * Instead of RAMDirectory we use ByteBufferDirectory
        */
        Directory index = new ByteBuffersDirectory();

        /*
        * 3. Create IndexWriter Configuration
        */
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        /*
         * 4. Create IndexWriter - Adds the document to the index
         */
        IndexWriter writer = new IndexWriter(index, config);

        addDocument(writer, "Java Programming", "Java is best object oriented programming");
        addDocument(writer, "Java Concepts", "Java is a powerful programming");
        addDocument(writer, "Java OOPS", "OOPS are must have in any programming");
        addDocument(writer, "Java Detail", "Java offers best detailing for OOPS");
        writer.close();

        /*
         *  Searching
         *  QueryParser - Parses the search query, target the content field
         */
        String queryString = "Java";
        QueryParser parser = new QueryParser("content", analyzer);
        Query query = parser.parse(queryString);

        /*
         * IndexSearcher - Performs the search on the index
         * DirectoryReader - Reads the indexed documents for searching
         */
        DirectoryReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);

        // TopDocs - Holds the search results
        TopDocs results = searcher.search(query, 2);

        System.out.println("Total Hits : " + results.totalHits.value);
        for(ScoreDoc score: results.scoreDocs) {
            // Relevance Scoring - for each document is computed based on Lucene's scoring algorithm
            // Example - TF-IDF (Term Frequency - Inverse Document Frequency)
            Document doc = searcher.doc(score.doc);
            System.out.println("Title: " + doc.get("title") + " and " + "score : " + score.score);
        }


    }
}
