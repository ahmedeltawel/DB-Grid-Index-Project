import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

public class helper {

    public static int readConfig(String keyname) {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {
            System.out.println("error file path incorrect!");
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            System.out.println("error loading the config file");
        }

        String property = prop.getProperty(keyname);

        return Integer.parseInt(property);
    }

    public static void serialize(Object obj, String fileLocation) {
        try {
            FileOutputStream fileOut = new FileOutputStream(fileLocation);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(obj);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public static Object deserialize(String fileLocation) throws Exception {
        Object obj = null;
        FileInputStream fileIn = new FileInputStream(fileLocation);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        obj = in.readObject();
        in.close();
        fileIn.close();

        return obj;
    }

    public static void renameFile(String FilePath, String newFilePath) throws IOException {
        // full pathes for example, renameFile("src/main/java/Storage/Studentss",
        // "src/main/java/Storage/Students");
        Path source = Paths.get(FilePath);
        Path target = Paths.get(newFilePath);
        Files.move(source, target);
    }

    public static void delFile(String filepath) throws Exception {
        File f = new File(filepath);
        if (f.delete()) {
            // File is deleted successfully !
        } else {
            throw new Exception("Failed to delete the file " + filepath);
        }
    }

    private static final Comparator<Number> NUMBER_COMPARATOR = new Comparator<Number>() {
        private BigDecimal createBigDecimal(Number value) {
            BigDecimal result = null;
            if (value instanceof Short) {
                result = BigDecimal.valueOf(value.shortValue());
            } else if (value instanceof Long) {
                result = BigDecimal.valueOf(value.longValue());
            } else if (value instanceof Float) {
                result = BigDecimal.valueOf(value.floatValue());
            } else if (value instanceof Double) {
                result = BigDecimal.valueOf(value.doubleValue());
            } else if (value instanceof Integer) {
                result = BigDecimal.valueOf(value.intValue());
            } else {
                throw new IllegalArgumentException("unsupported Number subtype: " + value.getClass().getName());
            }
            assert (result != null);

            return result;
        }

        public int compare(Number o1, Number o2) {
            return createBigDecimal(o1).compareTo(createBigDecimal(o2));
        };
    };

    public static int compareObjects(Object obj1, Object obj2) {
        /***
         * if s1 > s2, it returns positive number if s1 < s2, it returns negative number
         * if s1 == s2, it returns 0
         */
        // Primitive data types can't be compared using compareTo()
        if (obj1 instanceof Number && obj2 instanceof Number) {
            return NUMBER_COMPARATOR.compare((Number) obj1, (Number) obj2);
        }
        return ((Comparable) obj1).compareTo((Comparable) obj2);
    }

    public static Object parseString(String str) {
        // Convert from string to an actual comparable object
        // java.lang.Integer, java.lang.String, java.lang.Double, java.util.Date

        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date date = format.parse(str);
            if (date != null)
                return date;
        } catch (Exception e) {
        }
        try {
            Integer intValue = Integer.parseInt(str);
            if (intValue != null)
                return intValue;
        } catch (Exception e) {
        }
        try {
            Double dbl = Double.parseDouble(str);
            if (dbl != null)
                return dbl;
        } catch (Exception e) {
        }

        return str;
    }

    public static Object parseStringButRespectType(String string, String Type) throws Exception {
        // Convert from string to an actual comparable object
        // java.lang.Integer, java.lang.String, java.lang.Double, java.util.Date
        try {
            switch (Type.toLowerCase()) {
                case "java.lang.integer":
                    return Integer.parseInt(string);
                case "java.lang.string":
                    return string;
                case "java.lang.double":
                    return Double.parseDouble(string);
                case "java.util.date":
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    return format.parse(string);

                default:
                    throw new Exception("Invalid Type while parsing");
            }
        } catch (ParseException | NumberFormatException e1) {
            return parseString(string);
        }
    }

    public static void createFolder(String path) {
        File directory = new File(path);
        if (!directory.exists()) {
            directory.mkdir();
            // If you require it to make the entire directory path including parents,
            // use directory.mkdirs(); here instead.
        }
    }

    public static void createFile(String path) {
        File file = new File(path);
        if (!file.exists())
            try {
                file.createNewFile();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
    }

    public static void addTableToMetaDataFile(Table t) {
        String filepath = "src/main/resources/metadata.csv";

        try {
            File metaDataFile = new File(filepath);
            FileWriter writer = new FileWriter(metaDataFile, true);

            if (!metaDataFile.exists() | StringUtils.isEmpty(Files.readString(Path.of(filepath)))) {
                writer.write("Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max\n");
            }

            String[] lineSegments = new String[7];
            DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd");

            for (Column c : t.Columns) {
                lineSegments[0] = t.TableName;
                lineSegments[1] = c.name;
                lineSegments[2] = c.strType;
                lineSegments[3] = (c.isPK) ? "True" : "False";
                lineSegments[4] = (c.indexed) ? "True" : "False";
                lineSegments[5] = (c.MinValue instanceof java.util.Date) ? dtf.format((Date) c.MinValue)
                        : c.MinValue.toString();
                lineSegments[6] = (c.MaxValue instanceof java.util.Date) ? dtf.format((Date) c.MaxValue)
                        : c.MaxValue.toString();

                writer.write(convertToCSVLine(lineSegments) + "\n");
            }

            writer.close();
        } catch (Exception e) {
            System.out.println("An error occurred, While Writing to the metaDataFile");
            System.out.println(e.getMessage());
        }
    }

    private static String convertToCSVLine(String[] data) {
        return Stream.of(data).collect(Collectors.joining(","));
    }

    private static Integer bs2(int[] arr, int searchValue) {
        int lo = 0;
        int hi = arr.length;
        while (lo < hi) {
            int mid = Math.floorDiv(lo + hi, 2);
            if (searchValue < arr[mid]) {
                hi = mid; // left
            } else
                lo = mid + 1; // right
        }
        lo = lo - 1;
        // try {
        // if(arr[lo] == searchValue) return lo;
        // } catch (Exception e) {
        // }
        // return null;
        return lo;
    }

    public static void deleteDirectoryFiles(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                file.delete();
            }
        }

    }

    public static void main(String[] args) {
        Date dt  = new Date(2010 - 1900, 10 -1, 3);
        System.out.println(dt);
        /*
         * enumRows: for each row in rows{ enumCols: for each insertedCol in
         * insertedHashtable{ if row not empty{ for each rowCol in row{ if (rowCol ==
         * insertedCol){ if(insertedHashtable.get(rowCol) == row.get(rowCol)){
         * if(insetedCol is last in the insertedHashtable) delete(row); continue
         * enumCols; }else{ continue enumRows; } } } break; // column mismatches }
         * continue enumRows; } }
         */
    }

}
