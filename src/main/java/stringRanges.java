

// Java program to find the count of same length
// Strings that exists lexicographically
// in between two given Strings

class stringRanges {

    // Function to find the count of strings less
    // than given string lexicographically
    private static long LexicoLesserStrings(String s) {
        long count = 0;
        int len;

        // Find length of string s
        len = s.length();

        // Looping over the string characters and
        // finding strings less than that character
        for (int i = 0; i < len; i++) {
            count += (s.charAt(i) - 'a') * Math.pow(26, len - i - 1);
        }
        return count;
    }

    // Function to find the count of
    // same length Strings that exists
    // lexicographically in between two
    // given Strings
    private static long countString(String S1, String S2) {
        long countS1, countS2, totalString;

        // Count string less than S1
        countS1 = LexicoLesserStrings(S1);

        // Count string less than S2
        countS2 = LexicoLesserStrings(S2);

        // Total strings between S1 and S2 would
        // be difference between the counts - 1
        totalString = countS2 - countS1 - 1;

        // If S1 is lexicographically greater
        // than S2 then return 0, otherwise return
        // the value of totalString
        return (totalString < 0 ? 0 : totalString);
    }

    public static long countRange(String s1, String s2) {
        // BB
        // EEEE
        /**
         * BB->ZZ        0
         * AAA->ZZZ      1
         * AAAA->EEEE    2
         * 
         */
        if(s1.length() == s2.length()) return countString(s1, s2);

        int finalResult = 0;

        for (int i = 0; i <= s2.length() -s1.length() ; i++) {
            
            String start = "";
            String end = "";
                        
            if(i==0){ // special start
                start = s1;
                int length = s1.length() + i;
                for (int j = 0; j < length; j++) 
                    end += "Z";
            }else
            if(i==( s2.length() -s1.length())){ // special end
                end = s2;
                int length = s2.length();
                for (int j = 0; j < length; j++) 
                    start += "A";
            }else{
                int length = s1.length() + i;
                for (int j = 0; j < length; j++) {
                    start += "A";
                    end += "Z";
                }

            }
            finalResult += countString(start, end) +2;
        }
      
        return finalResult;
    }

    // Driver code
    public static void main(String args[]) {

        // System.out.println(countRange("BB", "AZHI"));
        System.out.println(countRange("78-002", "78-005"));
        System.out.println(countRange("63-3985", "71-3690"));
        // System.out.println(countRange("78-2385", "78-2390"));
        

    }
}

// This code is contributed by apurva raj
