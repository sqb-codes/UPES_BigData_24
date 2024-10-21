package org.example.CustomAnalyzer;

import org.apache.lucene.analysis.Analyzer;
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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.IOException;

public class App {

    public static void addDocument(IndexWriter writer, String title, String content) throws IOException {
        Document doc = new Document();
        doc.add(new TextField("title", title, TextField.Store.YES));
        doc.add(new TextField("content", content, TextField.Store.YES));
        writer.addDocument(doc);
    }

    public static void main(String[] args) throws IOException, ParseException {
        Analyzer analyzer = new CustomAnalyzer();
        Directory index = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(index, config);

        addDocument(writer, "Java Programming", "Java is best object oriented programming");
        addDocument(writer, "Java Concepts", "Java is a powerful programming");
        addDocument(writer, "Java OOPS", "OOPS are must have in any programming");
        addDocument(writer, "Java Detail", "Java offers best detailing for OOPS");
        writer.close();

        String queryString = "Java";
        QueryParser parser = new QueryParser("content", analyzer);
        Query query = parser.parse(queryString);

        DirectoryReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);

        // TopDocs - Holds the search results
        TopDocs results = searcher.search(query, 2);

        System.out.println("Total Hits : " + results.totalHits.value);
        for(ScoreDoc score: results.scoreDocs) {
            // Relevance Scoring - for each document is computed based on Lucene's scoring algorithm
            // Example - TF-IDF (Term Frequency - Inverse Document Frequency)
            Document doc = searcher.doc(score.doc);
            System.out.println("Title: " + doc.get("title") + " and " + "Score : " + score.score);
        }

    }
}
