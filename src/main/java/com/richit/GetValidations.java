package com.richit;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.protocol.types.Field;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import scala.math.Ordering;

public class GetValidations {
    //Metodo para encontrar el delimitador en los Header
    public static String FindDelimH(String header){
        int i, cequal=0;
        String aux = "", result="";
        String [] listd = {",",";","|"," ","\t","-"};
        Object [][] matrixd = new Object[listd.length][3];

        //Se llena la matriz
        for(i=0;i<listd.length;i++){
            matrixd[i][0]=listd[i];
            matrixd[i][1]=0;
            matrixd[i][2]=listd.length-i;
        }

        for (i=0; i<listd.length;i++) matrixd[i][1] = StringUtils.countMatches(header, listd[i]);

        for (i=0;i<listd.length;i++){
            if((Integer)matrixd[i][1]!=0){
                cequal++;
                aux = (String) matrixd[i][0];
                break;
            }
        }
        if (cequal==0) result="1 campo";
        else if (cequal>=1) result=aux;

        return result;
    }

    //Metodo para encontrar el delimitador en los datos
    public static String FindDelimD(String stream, int fields){
        int i, cmax=0,cmin=0,cequal=0,a=0,b=0;
        String dd = "";

        //Lista de delimitadores
        String [] listd = {",",";","|"," ","\t","-"};

        //Matriz de delimitadores, cantidad y jerarquía
        Object [][] matrixd = new Object[listd.length][3];
        for(i=0;i<listd.length;i++){
            matrixd[i][0]=listd[i];
            matrixd[i][1]=0;
            matrixd[i][2]=listd.length-i;
        }

        //Guardar en un arreglo las veces que se repite cada valor de los delimitadores
        for (i=0; i<listd.length;i++) matrixd[i][1] = StringUtils.countMatches(stream, listd[i]);
        //Cuantos delimitadores coinciden con los campos o no
        for (i=0;i<listd.length;i++){
            if ((Integer)matrixd[i][1] > fields) cmax++; //Mas campos de los esperados
            else if ((Integer)matrixd[i][1]<fields && (Integer)matrixd[i][1]!=0) cmin++; //Menos campos de los esperados
            else if ((Integer)matrixd[i][1]==fields) cequal++; //El delimitador se presenta las mismas veces que el número de campos
        }

        if(cmax==0){
            //Existe uno o mas delimitadores que coinciden con los campos (Caso 4)
            if (cmin==0 && cequal>0) {
                for (i=0;i<listd.length;i++) {
                    if ((Integer) matrixd[i][1] == fields) {
                        dd = (String) matrixd[i][0];
                        break;
                    }}}
            //Existen menos delimitadores que coincidan con los campos (Caso 5)
            //No existe ningún delimitador que coincida con los campos (Caso 3)
            else if (cequal==0 && cmin>=0) dd="1";
                //Existen uno o más delimitadores que coincidan con los campos y uno o más que son menos a los campos(Caso 7)
            else if (cmin>0 && cequal>0) {
                for (i = 0; i < listd.length; i++) {
                    if ((Integer) matrixd[i][1] == fields) {
                        a = (Integer) matrixd[i][2];
                        dd = (String) matrixd[i][0];
                    } else if ((Integer) matrixd[i][1] < fields && (Integer) matrixd[i][1] > 0)
                        b = (Integer) matrixd[i][2];
                }
                if (a <= b) dd="1";
            }}
        else{
            //Existen más delimitadores que los campos (Caso 6)
            //Existen más delimitadores que los campos y al mismo tiempo menos que los campos (Caso 8)
            if (cequal==0) dd="2";
                //Existen más delimitadores que los campos, a su vez menos delimitadores que los campos y uno o más delimitadores iguales a los campos(Caso 1)
                //Existen más delimitadores que los campos y a la vez uno o más delimitadores iguales a los campos (Caso 2)
            else if (cequal>0) {
                for (i = 0; i < listd.length; i++) {
                    if ((Integer) matrixd[i][1] == fields && fields!=0) {
                        a = (Integer) matrixd[i][2];
                        dd = (String) matrixd[i][0];
                    } else if ((Integer) matrixd[i][1] > fields) b = (Integer) matrixd[i][2];
                }
                if (a <= b) dd="2";
            }}

        return dd;
    }

    //Metodo para validar JSON
    public static boolean isJson(String Json) {
        try {
            new JSONObject(Json);
        } catch (JSONException ex) {
            try {
                new JSONArray(Json);
            } catch (JSONException ex1) {
                return false;
            }
        }
        return true;
    }
    //Se conoce el delimitador
    //Metodo para encontrar el delimitador en los Header
    public static Integer FindDelimH2(String header, String delim){
        int dh;
        dh = StringUtils.countMatches(header, delim);
        return dh;
    }

    //Metodo para encontrar el delimitador en los datos
    public static Integer FindDelimD2(String stream, int fields, String delim){
        int dd;
        //Guardar en un arreglo las veces que se repite cada valor de los delimitadores
        dd = StringUtils.countMatches(stream, delim);
        return dd;
    }
}
