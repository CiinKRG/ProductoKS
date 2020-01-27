package com.richit;

import java.io.File;
import java.lang.*;

import org.apache.commons.lang.StringUtils;
import org.apache.tika.Tika;
import org.json.CDL;
import org.json.JSONArray;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static com.richit.GetValidations.*;

public class ReadFile {
    public static void main(String[] args) throws IOException {
        //Para el caso donde el archivo no tiene header
        //Detectar tipo de archivo
        String file = "/home/cynthia/Documentos/ProductoKS/checksh.csv";
        File inputFile = new File(file);
        String type = new Tika().detect(inputFile);

        String header = "Col1,Col2,Col3,Col4";
        String dh = FindDelimH(header);
        String dd,fc;
        JSONArray array;

        //Cuantas veces el delimitador se encuentra en el header
        int fields = StringUtils.countMatches(header, dh);

        //Depende del tipo de archivo se procesa
        switch (type){
            case "text/csv":
                BufferedReader csvFile= new BufferedReader(new FileReader(file));
                String fileContent = csvFile.readLine();
                //Comparamos el numero de campos en header vs primera línea del archivo
                dd = FindDelimD(fileContent, fields);

                if (dd == "1") System.out.println("Se tienen menos campos");        //Enviar a tópico de error
                else if (dd == "2") System.out.println("Se tienen mas campos");     //Enviar a tópico de error
                else {
                    if (fields == 0) { //Toma el caso de un campo
                        fc = header + "\n" + fileContent;
                        array = CDL.toJSONArray(fc);
                        System.out.println(array.toString(2));
                        System.out.println(isJson(array.toString()));
                    } else {
                        //Cambia el delimitador del header por comas
                        if (dh!=",") header=header.replace(dh,",");
                        //Cambia la primera línea a tomar del archivo por comas
                        if (dd !=",") fileContent =fileContent.replace(dd, ",");
                        String fileContent2= fileContent;
                        while (fileContent !=null){
                            if (dd !=",") fileContent =fileContent.replace(dd, ",");
                            fileContent=csvFile.readLine();
                            if (fileContent != null){
                                fileContent2=fileContent2+"\n"+fileContent;
                            }
                        }
                        fileContent2=header+"\n"+fileContent2;
                        array = CDL.toJSONArray(fileContent2);
                        System.out.println(array.toString(2));
                        System.out.println(isJson(array.toString()));
                    }
                }
                break;

            case "text/plain":
                BufferedReader txtFile= new BufferedReader(new FileReader(file));
                /*String header = txtFile.readLine();
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
                */break;

            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }
}
