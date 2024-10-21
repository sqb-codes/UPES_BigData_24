package org.example.CustomAnalyzer;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import java.util.Arrays;


public class CustomAnalyzer extends Analyzer {

    private final CharArraySet STOPWORDS = new CharArraySet(Arrays.asList("is", "the", "or", "if"), true);

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        /* Split text based on spaces */
        WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
        /* Convert all tokens to lowercase, making the search case-insensitive */
        TokenStream filter = new LowerCaseFilter(tokenizer);
        /* Also adding removal of Stopwords from our filter */
        filter = new StopFilter(filter, STOPWORDS);
        return new TokenStreamComponents(tokenizer, filter);
    }
}
