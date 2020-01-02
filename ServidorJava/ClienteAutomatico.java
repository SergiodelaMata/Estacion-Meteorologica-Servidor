import java.io.*;
import java.net.*;
import java.util.concurrent.Callable;

import static java.lang.Thread.sleep;


public class ClienteAutomatico implements Callable<String> {

    Socket cliente;

    DataInputStream entrada;
    DataOutputStream salida;

    String mensaje, respuesta;

    private String idConexion;

    public ClienteAutomatico(String id){
        this.idConexion = id;
        this.respuesta = "NULL";
    }

    @Override
    public String call(){

        try{

            try {
                sleep(180000);
            } catch (InterruptedException e) {
                System.out.println("ErrorIE: "+ e.getMessage());
            }

            cliente = new Socket("weatherubicuastation.duckdns.org", 8080);

            entrada = new DataInputStream(cliente.getInputStream());
            salida = new DataOutputStream(cliente.getOutputStream());

            mensaje = "Refresh-"+this.idConexion;

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