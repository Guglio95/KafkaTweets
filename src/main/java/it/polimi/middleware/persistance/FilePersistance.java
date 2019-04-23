package it.polimi.middleware.persistance;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class FilePersistance {
    private static final Logger logger = Logger.getLogger(FilePersistance.class);

    private String filename;

    FilePersistance(String filename) {
        this.filename = "/tmp/" + filename;
    }

    void appendLine(String line) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.filename, true));) {
            writer.append(line + "\n");
            writer.flush();
            logger.info("Appending to file " + filename + " line " + line);
        } catch (IOException e) {
            logger.error("Error while writing to file " + filename, e);
        }
    }

    void storeValue(String value) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.filename, false));) {
            writer.write(value);
            writer.flush();
            logger.info("Writing to file " + filename + " line " + value);
        } catch (IOException e) {
            logger.error("Error while writing to file " + filename, e);
        }
    }

    Long getLine() {
        if (!new File(filename).exists()) return null;

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String last = null, line;

            while ((line = reader.readLine()) != null) {
                last = line;
            }

            if (last == null) {
                logger.info("File " + filename + " does not contain an offset because it's empty.");
                return null;
            }
            return Long.valueOf(last);
        } catch (IOException e) {
            logger.error("Error reading offset line from " + filename, e);
            return null;
        }
    }

    <T> List<T> readAll(Class<T> type) {
        if (!new File(filename).exists()) return Collections.emptyList();

        Gson gson = new Gson();
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            logger.info("Reading all stored lines from " + filename);
            return reader.lines()
                    .map(line -> gson.fromJson(line, type))
                    .collect(Collectors.toList());

        } catch (IOException e) {
            logger.error("IOException reading lines from " + filename, e);
            return Collections.emptyList();
        }
    }
}