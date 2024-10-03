package org.example;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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

    public static void main(String[] args) throws IOException {
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

    }
}
