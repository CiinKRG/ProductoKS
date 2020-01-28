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
        String file = "/home/cynthia/Documentos/ProductoKS/files/test1fh.txt";
        File inputFile = new File(file);
        String type = new Tika().detect(inputFile);

        JSONArray array;
        String header,delim,fileC,fileC2;
        BufferedReader fileR= new BufferedReader(new FileReader(file)); //Lectura del archivo

        System.out.println(type);

        if ((type.equals("text/csv")) || (type.equals("text/plain"))){
            header = fileR.readLine();
            delim = FindDelimH(header);

            if (delim!=",") header=header.replace(delim,",");
            fileC = header;
            fileC2 = fileC;

            while (fileC !=null){
                fileC=fileR.readLine();
                if (fileC != null){
                    if (delim!=",") fileC=fileC.replace(delim,",");
                    fileC2=fileC2+"\n"+fileC;
                }
            }
            array = CDL.toJSONArray(fileC2);
            System.out.println(array.toString(2));
            System.out.println(isJson(array.toString()));
        }
        else if (type.equals("application/json")){ //Valida la estructura
            String message = org.apache.commons.io.IOUtils.toString(fileR);
            System.out.println(message);
            System.out.println(isJson(message));
        }

    }
}
