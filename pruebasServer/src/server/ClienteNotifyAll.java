/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package server;

/**
 *
 * @author Alberto
 */
import java.io.*;
import java.net.*;
import java.util.concurrent.Callable;


public class ClienteNotifyAll implements Callable<String> {

    Socket cliente;

    DataInputStream entrada;
    DataOutputStream salida;

    String mensaje, respuesta;

    private String idConexion;

    public ClienteNotifyAll(String id){
        this.idConexion = id;
        this.respuesta = "NULL";
    }

    @Override
    public String call(){

        try{

            cliente = new Socket("weatherubicuastation.duckdns.org", 8080);

            entrada = new DataInputStream(cliente.getInputStream());
            salida = new DataOutputStream(cliente.getOutputStream());

            mensaje = "NotifyAll-"+"5";

            salida.writeUTF(mensaje);

            respuesta = entrada.readUTF();
            System.out.println(respuesta);

            entrada.close();
            salida.close();
            cliente.close();

        }catch(IOException e){

            System.out.println("Error: "+e.getMessage());
        }

        return respuesta;
    }
}

