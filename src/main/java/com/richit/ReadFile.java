package com.richit;

import java.nio.file.Path;
import java.util.*;
import java.lang.*;
import java.math.*;
import java.io.*;
import java.io.File;
import java.nio.charset.*;
import java.util.*;
import java.util.Map.*;
import java.util.Arrays;
import java.util.List;
import java.nio.charset.*;
import java.util.Collections;

import com.fasterxml.jackson.dataformat.csv.CsvParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.spark.InternalAccumulator;
import org.apache.tika.Tika;
import org.json.CDL;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONTokener;
import org.omg.PortableInterceptor.ServerRequestInfo;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ReadFile {

    public static void main(String[] args) throws IOException {
        //Leer archivo CSV o trama (separada por espacio) STREAM
        //Identificar que tipo de archivo es
        //Darle formato JSON
        //Enviar a topico
        //Los errores se envían a otro tópico con un campo extra que indique el error
        //Detectar tipo de separador en csv

        //---------------------------------------------------------
        //Detectar tipo de archivo
        //----------------------------------------------------------
        /*
        File inputFile = new File("/home/cynthia/Documentos/ProductoKS/test2.txt");
        String type = new Tika().detect(inputFile);
        System.out.println(type);
        */

        //---------------------------------------------------------
        //Leer archivo
        //----------------------------------------------------------
        //Salida en detección de archivo: text/csv
        //Leer CSV
        //Misma función para leer txt y Json
        /*
        Path logFile = Paths.get("/home/cynthia/Documentos/ProductoKS/test1.json");
        try (BufferedReader reader =
                     Files.newBufferedReader(logFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        */

        //---------------------------------------------------------
        //Convertir a JSON con header
        //----------------------------------------------------------

        //CSV to JSON
        /*
        BufferedReader csvFile= new BufferedReader(new FileReader("/home/cynthia/Documentos/ProductoKS/test2.txt"));

        String fileContent = csvFile.readLine();
        String fileContent2= fileContent;

        while (fileContent !=null){
            fileContent=csvFile.readLine();
            if (fileContent != null){
                fileContent2=fileContent2+"\n"+fileContent;
            }
        }

        JSONArray array = CDL.toJSONArray(fileContent2);
        System.out.println(array.toString(2)); //pretty print with indent
        */

        //Txt to JSON

        //BufferedReader csvFile= new BufferedReader(new FileReader("/home/cynthia/Documentos/ProductoKS/test2.txt"));
        Reader in = new FileReader("/home/cynthia/Documentos/ProductoKS/test2.txt");
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);

        String resul = "ID_SEQ ID_ACCESS RAMO SUBRAMO TIPO_TRAMITE CARPETA";

        int i,cmax=0,cmin=0,cequal=0,aux=0;
        String auxd="";
        String [] listd = {",",";","|"," ","\t","-"};
        Object [][] matrixd = new Object[listd.length][3];

        for(i=0;i<listd.length;i++){
            matrixd[i][0]=listd[i];
            matrixd[i][1]=0;
            matrixd[i][2]=listd.length-i;
        }

        for (i=0; i<listd.length;i++) matrixd[i][1] = StringUtils.countMatches(resul, listd[i]);

        for (i=0;i<listd.length;i++){
            if((Integer)matrixd[i][1]==0) cmin=cmin;
            else {
                cequal++;
                auxd = (String) matrixd[i][0];
            }
        }

        if (cequal==0) System.out.println("El header solo tiene un campo");
        else if (cequal==1) System.out.println("El header es: \""+auxd+"\"");
        else {
            
        }


        //for (CSVRecord record: records) System.out.println(record);

        //---------------------------------------------------------
        //Detectar delimitador
        //----------------------------------------------------------
        /*
        //Indica cuantos campos del header son
        int i, a=0, b=0, cmax = 0, cmin = 0, cequal = 0, c_one=0;
        String dd = ""; //Define el delimitador de los datos entrantes
        String header = "Col1,Col2,Col3,Col4";
        String dh = ",";
        int fields = StringUtils.countMatches(header, dh);

        //Lista de delimitadores
        String [] listd = {",",";","|"," ","\t","-"};

        //Matriz de delimitadores, cantidad y jerarquía
        Object [][] matrixd = new Object[listd.length][3];
        for(i=0;i<listd.length;i++){
            matrixd[i][0]=listd[i];
            matrixd[i][1]=0;
            matrixd[i][2]=listd.length-i;
        }

        //Pruebas de varias entradas de datos
        String ex1 = "datoA,datoB";                                 //Menos campos
        String ex2 = "a,b,c,d,e,f,g";                               //Mas campos
        String ex3 = "dato1,dato2,dato3,dato4";                     //Coincide
        String ex4 = "d1;d2;d3;d4";                                 //Coincide
        String ex5 = "a b c d";                                     //Coincide
        String ex6 = "1|2|3|4";                                     //Coincide
        String ex7 = ",,,1";                                        //Coincide
        String ex8 = "as\tdc\ted\tqw";                              //Coincide
        String ex9 = "data1,data2-a,data2-b,data3";                 //Coincide, incluye guiones
        String ex10 = "1-a,1-b,1-c,1-d";                            //Coincide, incluye comas y guiones pero el delimitador es por comas
        String ex11 = "a-1,a-2,a-3,b";                              //Coincide, misma cantidad de guiones y comas
        String ex12 ="q-1,q-2,q-3,a4,dw,sds";                       //No coincide, incluye guiones y comas
        String ex13 ="a-b-c,d-1";                                   //No coincide, incluye comas y guiones
        String ex14 ="a-b-c-d-e,1-e,2-e,3";                         //Coincide
        String ex15 ="abc dfdf de la defd;24;col ssjfjd;del-jdjdjd";//Coincide (jerarquía)
        String ex16 = "a";                                          //Menos campos

        //Guardar en un arreglo las veces que se repite cada valor de los delimitadores
        for (i=0; i<listd.length;i++) {
            matrixd[i][1] = StringUtils.countMatches(ex15, listd[i]);
            //System.out.println("El valor:" + matrixd[i][0] + " se repite " + matrixd[i][1]+ " veces" + " tiene un peso de: "+ matrixd[i][2]);
        }

        //Cuantos delimitadores coinciden con los campos o no
        for (i=0;i<listd.length;i++){
            //Mas campos de los esperados
            if ((Integer)matrixd[i][1] > fields) cmax++;
            //Menos campos de los esperados
            else if ((Integer)matrixd[i][1]<fields && (Integer)matrixd[i][1]!=0) cmin++;
            //El delimitador se presenta las mismas veces que el número de campos
            else if ((Integer)matrixd[i][1]==fields) cequal++;
        }
        //System.out.println("Contador mayores: "+cmax+"\nContador menores: "+cmin+"\nContador iguales: "+cequal);


        if(cmax==0){
            //Existe uno o mas delimitadores que coinciden con los campos (Caso 4)
            if (cmin==0 && cequal>0) {
                for (i=0;i<listd.length;i++) {
                    if ((Integer) matrixd[i][1] == fields) {
                        dd = (String) matrixd[i][0];
                        System.out.println("El delimitador es: \"" + dd + "\"");
                        break;
                    }}}
            //Existen menos delimitadores que coincidan con los campos (Caso 5)
            //No existe ningún delimitador que coincida con los campos (Caso 3)
            else if (cequal==0 && cmin>=0) System.out.println("Menos campos de los esperados");
            //Existen uno o más delimitadores que coincidan con los campos y uno o más que son menos a los campos(Caso 7)
            else if (cmin>0 && cequal>0) {
                for (i = 0; i < listd.length; i++) {
                    if ((Integer) matrixd[i][1] == fields) {
                        a = (Integer) matrixd[i][2];
                        dd = (String) matrixd[i][0];
                    } else if ((Integer) matrixd[i][1] < fields && (Integer) matrixd[i][1] > 0)
                        b = (Integer) matrixd[i][2];
                }
                if (a > b) System.out.println("El delimitador es: \"" + dd + "\"");
                else System.out.println("Menos campos de los esperados");
            }}
        else{
            //Existen más delimitadores que los campos (Caso 6)
            //Existen más delimitadores que los campos y al mismo tiempo menos que los campos (Caso 8)
            if (cequal==0) System.out.println("Más campos de los esperados");
            //Existen más delimitadores que los campos, a su vez menos delimitadores que los campos y uno o más delimitadores iguales a los campos(Caso 1)
            //Existen más delimitadores que los campos y a la vez uno o más delimitadores iguales a los campos (Caso 2)
            else if (cequal>0) {
                for (i = 0; i < listd.length; i++) {
                    if ((Integer) matrixd[i][1] == fields) {
                        a = (Integer) matrixd[i][2];
                        dd = (String) matrixd[i][0];
                    } else if ((Integer) matrixd[i][1] > fields) b = (Integer) matrixd[i][2];
                }
                if (a > b) System.out.println("El delimitador es: \"" + dd + "\"");
                else System.out.println("Mas campos de los esperados");
            }}
        */
        //---------------------------------------------------------
        //Convertir a JSON
        //----------------------------------------------------------
        //Stream to JSON
        /*
        String x="";
        if(dd!=",") x=ex15.replace(dd,",");

        String fileContent2 = header + "\n" + x;
        JSONArray array = CDL.toJSONArray(fileContent2);
        System.out.println(array.toString(2));
        */
    }
}
