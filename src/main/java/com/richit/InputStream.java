package com.richit;

import org.apache.commons.lang.StringUtils;
import org.json.CDL;
import org.json.JSONArray;

import static com.richit.GetValidations.*;

public class InputStream {
    public static void main(String[] args) {

        String header = "Col1,Col2,Col3,Col4";
        //String header = "Column1";
        String dh = FindDelimH(header);
        String dd;

        //Cuantas veces el delimitador se encuentra en el header
        int fields = StringUtils.countMatches(header, dh);

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
        String ex12 = "q-1,q-2,q-3,a4,dw,sds";                      //No coincide, incluye guiones y comas
        String ex13 = "a-b-c,d-1";                                  //No coincide, incluye comas y guiones
        String ex14 = "a-b-c-d-e,1-e,2-e,3";                        //Coincide
        String ex15 = "abc dfdf de la defd;24;col ssjd;del-jdjd";   //Coincide (jerarquía)
        String ex16 = "a";                                          //Menos campos

        //Escoger el dato de prueba
        String data = ex15;

        //Regresa delimitador, un 1 si son menos campos y un 2 si son más campos
        dd = FindDelimD(data, fields);

        //Convierte a JSON si coincide
        if (dd == "1") System.out.println("Se tienen menos campos");        //Enviar a tópico de error
        else if (dd == "2") System.out.println("Se tienen mas campos");     //Enviar a tópico de error
        else {
            if (fields == 0) { //Toma el caso de un campo
                String fc = header + "\n" + data;
                JSONArray array = CDL.toJSONArray(fc);
                System.out.println(array.toString(2));
            } else {
                //Cambia el delimitador del header por comas
                if (dh!=",") header=header.replace(dh,",");
                //System.out.println("El delimitador es \"" + dd + "\"");
                if (dd != ",") data = data.replace(dd, ",");
                String fc = header + "\n" + data;
                JSONArray array = CDL.toJSONArray(fc);
                System.out.println(array.toString(2));
            }
        }
    }
}