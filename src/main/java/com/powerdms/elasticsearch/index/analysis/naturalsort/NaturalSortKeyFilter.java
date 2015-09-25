package com.powerdms.elasticsearch.index.analysis.naturalsort;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IndexableBinaryStringTools;

import java.io.IOException;
import java.text.Collator;
import java.util.regex.Pattern;


public final class NaturalSortKeyFilter extends TokenFilter {

    private final int MAX_NUM_DIGITS_IN_DIGIT_RUN = 15;

    private final Collator collator;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    /**
     * @param input    Source token stream
     * @param collator CollationKey generator
     */
    public NaturalSortKeyFilter(TokenStream input, Collator collator) {
        super(input);
        this.collator = (Collator) collator.clone();
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            byte[] collationKey = collator.getCollationKey(natural(termAtt.toString())).toByteArray();
            int encodedLength = IndexableBinaryStringTools.getEncodedLength(collationKey, 0, collationKey.length);
            termAtt.resizeBuffer(encodedLength);
            termAtt.setLength(encodedLength);
            IndexableBinaryStringTools.encode(collationKey, 0, collationKey.length, termAtt.buffer(), 0, encodedLength);
            return true;
        } else {
            return false;
        }
    }

    private String natural(String s) {
        if (s == null || s.isEmpty())
        {
            return "";
        }
        boolean isInDigitRun = false;
        StringBuilder digitRun = new StringBuilder(MAX_NUM_DIGITS_IN_DIGIT_RUN);
        StringBuilder sb = new StringBuilder(s.length());
        char[] chars = s.toLowerCase().toCharArray();
        for (char character : chars)
        {
            boolean newCharInDigitRun = isDigit(character);
            if (newCharInDigitRun)
            {
                // Continue the digit run, append the digit to the current digit run and continue
                digitRun.append(character);
            }
            if (isInDigitRun && !newCharInDigitRun)
            {
                // Digit run has finished, if there was one. Time to convert the digit run!
                String digitRunStr = convertToNaturalSortString(digitRun.toString());
                sb.append(digitRunStr);
                digitRun = new StringBuilder(MAX_NUM_DIGITS_IN_DIGIT_RUN);
            }
            if (!newCharInDigitRun)
            {
                if (character == ' ') {
                    sb.append('.');
                }
                else {
                    sb.append(character);
                }

            }
            isInDigitRun = newCharInDigitRun;
            if (digitRun.length() >= MAX_NUM_DIGITS_IN_DIGIT_RUN)
            {
                // Digit is too big, cut it here.
                String digitRunStr = convertToNaturalSortString(digitRun.toString());
                sb.append(digitRunStr);
                digitRun = new StringBuilder(MAX_NUM_DIGITS_IN_DIGIT_RUN);
                isInDigitRun = false;
            }
        }
        if (digitRun.length() > 0)
        {
            String digitRunStr = convertToNaturalSortString(digitRun.toString());
            sb.append(digitRunStr);
        }
        return sb.toString();
    }

    private boolean isDigit(char c)
    {
        switch (c)
        {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return true;
            default:
                return false;
        }
    }

    private String convertToNaturalSortString(String digits)
    {
        int length = digits.length();
        int numZeros = MAX_NUM_DIGITS_IN_DIGIT_RUN - length;
        StringBuilder sb = new StringBuilder(MAX_NUM_DIGITS_IN_DIGIT_RUN);
        for (int _ = 0; _ < numZeros; _++) {
            sb.append('0');
        }
        sb.append(digits);
        return sb.toString();
    }
}
