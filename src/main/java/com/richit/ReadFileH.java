package com.richit;

import java.io.File;
import java.lang.*;
import org.apache.tika.Tika;
import org.json.CDL;
import org.json.JSONArray;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static com.richit.GetValidations.*;

public class ReadFileH {
    public static void main(String[] args) throws IOException {
        //Detectar tipo de archivo
        String file = "/home/cynthia/Documentos/ProductoKS/test3.txt";
        File inputFile = new File(file);
        String type = new Tika().detect(inputFile);
        JSONArray array;

        //Depende del tipo de archivo se procesa
        switch (type){
            case "text/csv":
                BufferedReader csvFile= new BufferedReader(new FileReader(file));
                String fileContent = csvFile.readLine();
                String fileContent2= fileContent;

                while (fileContent !=null){
                    fileContent=csvFile.readLine();
                    if (fileContent != null){
                        fileContent2=fileContent2+"\n"+fileContent;
                    }
                }

                array = CDL.toJSONArray(fileContent2);
                System.out.println(array.toString(2));
                System.out.println(isJson(array.toString()));
                break;

            case "text/plain":
                BufferedReader txtFile= new BufferedReader(new FileReader(file));
                String header = txtFile.readLine();
                String delim =FindDelimH(header);
                String fileC = header, fileC2 = fileC;

                if (delim!=",") header=header.replace(delim,",");

                while (fileC !=null){
                    fileC=txtFile.readLine();
                    if (fileC != null){
                        fileC=fileC.replace(delim,",");
                        fileC2=fileC2+"\n"+fileC;
                    }
                }

                array = CDL.toJSONArray(fileC2);
                System.out.println(array.toString(2));
                System.out.println(isJson(array.toString()));
                break;

            case "application/json":
                BufferedReader jsonFile= new BufferedReader(new FileReader(file));
                String message = org.apache.commons.io.IOUtils.toString(jsonFile);

                System.out.println(message);
                System.out.println(isJson(message));
                break;

            default:
                throw new IllegalStateException("Unexpected value: " + type);

        }
    }
}
