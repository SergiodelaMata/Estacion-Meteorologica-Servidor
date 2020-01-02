import java.io.*;
import java.net.*;

public class Servidor{

    public static void main(String[] args) {
        
        ServerSocket servidor;

        Socket conexion;

        System.out.print("Inicializando servidor... ");

        try {

            servidor = new ServerSocket(8080);

            while (true) {
                conexion = servidor.accept();
                System.out.println("\t[OK]");

                System.out.println("Nueva conexión entrante: " + conexion);

                ((ServidorHilo) new ServidorHilo(conexion)).start();

            }
        } catch (IOException ex) {

            System.out.println("Error: "+ex.getMessage());
        }
    }
}