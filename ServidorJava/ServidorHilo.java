import java.io.*;
import java.net.*;
import java.sql.*;

public class ServidorHilo extends Thread{

    private Socket conexion;

    private DataOutputStream salida;
    private DataInputStream entrada;
    
    private Connection conexionBD;
    
    private String dominio, usuario, password;

    public ServidorHilo(Socket socket) {

        this.conexion = socket;
        this.dominio = "jdbc:mysql://localhost:3306/estacion_meteorologica_inteligente"; //Cambiar el dominio en funcion del nombre de la base de datos
        this.usuario = "root";
        this.password = "WeatherStationUbicua2019"; 

        try {

            salida = new DataOutputStream(conexion.getOutputStream());
            entrada = new DataInputStream(conexion.getInputStream());
            
            Class.forName("com.mysql.jdbc.Driver");

        } catch (IOException ex) {

            System.out.println("ErrorIO: "+ex.getMessage());
        } catch (ClassNotFoundException ex) {
            
            System.out.println("ErrorCNF: "+ex.getMessage());
        }
    }

    public void desconectar() {
        try {

            conexion.close();

        } catch (IOException ex) {

            System.out.println("ErrorIO: "+ex.getMessage());
        }
    }

    @Override
    public void run() {

        String parametros = "";

        try {

            parametros = entrada.readUTF();
            
            String[] tokens = parametros.split("-");
            
            System.out.println(tokens[0]+tokens[1]);
        
            //Aqui veriamos el id de la estacion para realizar la consulta en la base de datos y devolver el JSON
            if(tokens[0].equals("Refresh")){
                
                try{
                    conectarBD();
                    String mensaje = refreshDatos(tokens[1]);
                    System.out.println(mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                }catch(SQLException ex){                  
                    System.out.println("ErrorSQL: "+ ex.getMessage());
                }
            }else if(tokens[0].equals("HistoricHigh")){
                try {
                    conectarBD();
                    String mensaje = historicInformationHigh(tokens[1]);
                    System.out.println("Dato de "+ tokens[1] +" más alto resigtrado "+mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: "+ ex.getMessage());
                }
            }else if(tokens[0].equals("HistoricLow")){
                try {
                    conectarBD();
                    String mensaje = historicInformationLow(tokens[1]);
                    System.out.println("Dato de "+ tokens[1] +" más bajo resigtrado "+mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: "+ ex.getMessage());
                }
            }
            //Aqui pondriamos más posibles acciones que puede ejecutar el servidor por ejemplo actualización de tablas...

        } catch (IOException ex) {

            System.out.println("ErrorIO: "+ex.getMessage());
        }

        desconectar();
    }
    
    public void conectarBD() throws SQLException{
            
        conexionBD = DriverManager.getConnection(dominio, usuario, password);
    }    
    
    public void desconectarBD() throws SQLException{
        
        conexionBD.close();
    }
    
    public String historicInformationHigh(String dato) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select MAX("+ dato +") from datos_recabados");
        
        String resultado = "";
        
        while(consulta.next()){
            resultado = consulta.getString(1);
        }
        
        return resultado;
    }
    
    public String historicInformationLow(String dato) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select MIN("+ dato +") from datos_recabados");
        
        String resultado = "";
        
        while(consulta.next()){
            resultado = consulta.getString(1);
        }
        
        return resultado;
    }
    
    public String refreshDatos(String idEstacion) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select * from datos_recabados where ID_Estacion="+Integer.parseInt(idEstacion)+" limit "+ 1); //Cambiar segun la base de datos
        
        String resultado = "";
        while(consulta.next()){
            
            for(int i=1; i<=14; i++){
                //Los ifs de dentro hacen que la ultima columna de la consulta no tenga el caracter separador
                if(consulta.getString(i)!=null){
                    if(i==14){
                        resultado += consulta.getString(i);
                    }else{
                        resultado += consulta.getString(i) + "//";
                    }
                }else{
                    if(i==14){
                        resultado += "NULL";
                    }else{
                        resultado += "NULL//";
                    }
                }
            }
            
            resultado += "\n";
        }
        
        return resultado;
    }
}
