package com.powerdms.elasticsearch.index.analysis.naturalsort;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IndexableBinaryStringTools;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.Collator;
import java.util.Arrays;


public final class NaturalSortKeyFilter extends TokenFilter {

    private final int MAX_NUM_DIGITS_IN_DIGIT_RUN = 20;
    private final int SPACE_CHARACTER = 32;
    private final int MAX_LENGTH = 26000;

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
            byte[] collationKey = natural(termAtt.toString());
            int encodedLength = IndexableBinaryStringTools.getEncodedLength(collationKey, 0, collationKey.length);
            termAtt.resizeBuffer(encodedLength);
            termAtt.setLength(encodedLength);
            IndexableBinaryStringTools.encode(collationKey, 0, collationKey.length, termAtt.buffer(), 0, encodedLength);
            return true;
        } else {
            return false;
        }
    }

    private byte[] natural(String s) throws UnsupportedEncodingException {
        if (s == null || s.isEmpty())
        {
            return new byte[0];
        }
        boolean isInDigitRun = false;
        StringBuilder digitRun = new StringBuilder(MAX_NUM_DIGITS_IN_DIGIT_RUN);
        StringBuilder sb = new StringBuilder(s.length());
        char[] chars = s.toLowerCase().toCharArray();
        for (char character : chars)
        {
            boolean newCharInDigitRun = Character.isDigit(character);
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

                if ((int) character <= SPACE_CHARACTER) {
                    sb.append("!!!");
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
        String retString = sb.toString();
        byte[] collatedBytes = collator.getCollationKey(retString).toByteArray();
        if (collatedBytes.length > MAX_LENGTH) {
            return Arrays.copyOf(collatedBytes, MAX_LENGTH);
        }
        return collatedBytes;
    }

    private String convertToNaturalSortString(String digits)
    {
        int length = digits.length();
        int numZeros = MAX_NUM_DIGITS_IN_DIGIT_RUN - length;
        StringBuilder sb = new StringBuilder(MAX_NUM_DIGITS_IN_DIGIT_RUN);
        for (int i = 0; i < numZeros; i++) {
            sb.append('0');
        }
        sb.append(digits);
        return sb.toString();
    }
}
