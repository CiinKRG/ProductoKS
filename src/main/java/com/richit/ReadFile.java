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
        //Para el caso donde el archivo no tiene header (CSV y TXT)

        //Detectar tipo de archivo
        String file = "/home/cynthia/Documentos/ProductoKS/files/checksh.csv";
        File inputFile = new File(file);
        String type = new Tika().detect(inputFile);

        String header = "Col1,Col2,Col3,Col4";
        String dh = FindDelimH(header);
        String dd,fc,fileContent2;
        JSONArray array;

        int fields = StringUtils.countMatches(header, dh); //Numero de campos de header

        BufferedReader FileH= new BufferedReader(new FileReader(file));
        String fileContent = FileH.readLine();
        dd = FindDelimD(fileContent, fields); //Numero de campos en header VS 1ra linea del archivo

        if (dd == "1") System.out.println("Se tienen menos campos"); //Enviar a tópico de error
        else if (dd == "2") System.out.println("Se tienen mas campos"); //Enviar a tópico de error
        else {
            if (fields == 0) { //Toma el caso de un campo
                fileContent2= fileContent;
                while (fileContent !=null){
                    fileContent=FileH.readLine();
                    if (fileContent != null) fileContent2=fileContent2+"\n"+fileContent;
                }
                fc = header + "\n" + fileContent2;
                array = CDL.toJSONArray(fc);
                System.out.println(array.toString(2));
                System.out.println(isJson(array.toString()));
            }
            else {
                if (dh!=",") header=header.replace(dh,","); //Cambia el delimitador del header
                if (dd !=",") fileContent =fileContent.replace(dd, ","); //Cambia la 1ra linea
                fileContent2= fileContent;
                while (fileContent !=null){
                    if (dd !=",") fileContent =fileContent.replace(dd, ",");
                    fileContent=FileH.readLine();
                    if (fileContent != null) fileContent2=fileContent2+"\n"+fileContent;
                }
                fc=header+"\n"+fileContent2;
                array = CDL.toJSONArray(fc);
                System.out.println(array.toString(2));
                System.out.println(isJson(array.toString()));
            }
        }
    }
}
