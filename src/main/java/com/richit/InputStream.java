package com.richit;

import org.apache.commons.lang.StringUtils;
import org.json.CDL;
import org.json.JSONArray;
import java.util.Properties;

import static com.richit.GetValidations.*;

public class InputStream {
    static Properties properties;
    public static void main(String[] args) {

        String filenameprops = args[0];
        GetPropertiesKafka getproperties = new GetPropertiesKafka();
        properties = getproperties.GetKafkaValues(filenameprops);

        String header = properties.getProperty("header");

        String dh = FindDelimH(header);
        int fields = StringUtils.countMatches(header, dh); //Cuantas veces el delimitador se encuentra en el header

        //Dato de prueba
        String data = "abc dfdf de la defd;24;col ssjd;del-jdjd";

        //Regresa delimitador, un 1 si son menos campos y un 2 si son más campos
        String dd = FindDelimD(data, fields);

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
                if (dd != ",") data = data.replace(dd, ",");
                String fc = header + "\n" + data;
                JSONArray array = CDL.toJSONArray(fc);
                System.out.println(array.toString(2));
            }
        }
    }
}