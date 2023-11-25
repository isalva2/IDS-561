package old;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;

public class MapReduce {

    public static void main(String[] args) {

        String path = "data/temperatures.txt";

        ArrayList<String> output = Preprocessing(path);

        ArrayList<List<String>> splits = Splitter(output, 2);

        for (List<String> split: splits) {
            System.out.println(split.size());
        }

        

    }

    static ArrayList<String> Preprocessing(String filePath) {
        try {
            // Read all lines from the text file
            ArrayList<String> lines = new ArrayList<>(Files.readAllLines(Path.of(filePath)));

            return lines;

        } catch (IOException e) {
            // Handle the exception (e.g., print an error message or log it)
            e.printStackTrace();

            return null;
        }
    }

    static ArrayList<List<String>> Splitter(ArrayList<String> input, int numSplits) {

        // get number of elements per split
        int splitSize = input.size() / numSplits;

        // return object of ArrayList of List<String>
        ArrayList<List<String>> splits = new ArrayList<>();

        for (int i = 0; i < numSplits; i++) {

            // start index
            int startIndex = i * splitSize;
            
            // instantiate end index
            int endIndex = 0;
            
            // index splits with last remaining split at the end of ArrayList
            if (i != numSplits - 1) {
                endIndex = (i + 1) * splitSize;
            } else {
                endIndex = input.size();
            }

            // create sublist of split
            List<String> split = input.subList(startIndex, endIndex);

            // add to splits
            splits.add(split);

        }

        return splits;

    }

}