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
        
            //Refresh-idEstacion
            //Hace una consulta para actualizar los datos mas recientes sobre una estacion
            if(tokens[0].equals("Refresh")){
                
                try{
                    conectarBD();
                    String mensaje = refreshDatos(Integer.parseInt(tokens[1]));
                    System.out.println(mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                }catch(SQLException ex){                  
                    System.out.println("ErrorSQL: "+ ex.getMessage());
                }
            //HistoricHigh-atributo
            //Devuelve el dato más alto registrado en todas las estaciones sobre un campo, ej: temperatura
            }else if(tokens[0].equals("HistoricHigh")){
                try {
                    conectarBD();
                    String mensaje = historicInformationHigh(tokens[1]);
                    System.out.println("Dato de "+ tokens[1] +" más alto registrado "+mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: "+ ex.getMessage());
                }
            //HistoricLow-atributo
            //Devuelve el dato más bajo registrado en todas las estaciones sobre un campo, ej: temperatura
            }else if(tokens[0].equals("HistoricLow")){
                try {
                    conectarBD();
                    String mensaje = historicInformationLow(tokens[1]);
                    System.out.println("Dato de "+ tokens[1] +" más bajo registrado "+mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: "+ ex.getMessage());
                }
            //RefreshAll-numeroEstaciones teniendo en cuenta que los id de estaciones van de 1 a n
            //Hace una consulta para actualizar los datos mas recientes sobre todas las estaciones
            }else if(tokens[0].equals("RefreshAll")){
                try{
                    conectarBD();
                    String mensaje = "";
                    //Nos pondrá los datos de cada estacion en una linea de diferente
                    for(int i=1; i<=Integer.parseInt(tokens[1]); i++){
                        mensaje += refreshDatosAll(i);
                    }
                    System.out.println(mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                }catch(SQLException ex){
                    System.out.println("ErrorSQL: "+ ex.getMessage());
                }
            //RefreshTable-numeroEstaciones teniendo en cuenta que los id de las estaciones van de 1 a n
            //Hace una consulta para refrecar los datos de la tabla (id, ubicacion, temperatura, humedad, presion)
            }else if(tokens[0].equals("RefreshTable")){
                try{
                    conectarBD();
                    String mensaje = "";
                    //El segundo parametro es el numero de estaciones que queremos
                    for(int i=1; i<=Integer.parseInt(tokens[1]); i++){
                        mensaje += refreshTable(i);
                    }
                    System.out.println(mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                }catch(SQLException ex){
                    System.out.println("ErrorSQL: "+ ex.getMessage());
                }
            //Notify-idEstacion 
            //Devuelve un String con alertas sobre los datos de una estacion. Si retorna NULL quiere decir que no hay ningun aviso si en 
            //cambio tiene otro contenido se debe mostrar la notificacion
            }else if(tokens[0].equals("Notify")){
                try{
                    conectarBD();
                    String mensaje = "";
                    mensaje += notifyAlert(Integer.parseInt(tokens[1]));
                    if(mensaje.isEmpty()){
                        System.out.println("No hay alertas");
                    }else{
                        System.out.println(mensaje);
                    }
                    salida.writeUTF(mensaje);
                    desconectarBD();
                }catch(SQLException ex){
                    System.out.println("ErrorSQL1: "+ ex.getMessage());
                }
            }

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
    
    public String alerts(int idEstacion, String columna, String valor) throws SQLException{
        
        String resultado = "";
        float dato = Float.parseFloat(valor);
        
        switch(columna){
            case "temperatura":
                //Avisos de temperatura que supera o es menor que x valor
                if(dato>=33){
                    resultado += "Alta temperatura: "+ dato +"ºC";
                }else if(dato<=5){
                    resultado += "Baja temperatura: "+ dato +"ºC";
                }
                
                int mayor = Integer.parseInt(historicInformationHigh(columna));
                int menor = Integer.parseInt(historicInformationLow(columna));
                
                //Avisos de maximo o minimos historicos en temperatura
                if(dato>=mayor){
                    resultado += " y se ha registrado un máximo histórico";
                }else if(dato<=menor){
                    resultado += " y se ha registrado un mínimo histórico";
                }
                
                break;
            default:
                break;
        }
        
        return resultado;
    }
    
    public String notifyAlert(int idEstacion) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select * from datos_recabados where ID_Estacion="+idEstacion+" limit "+ 1);
       
        //Cuando el resultado enviado a la app sea NULL querra decir que no hay ningun avsido que mostrar
        String resultado = "";
        
        while(consulta.next()){
            //********************************************************
            //CAMBIAR SI SE CAMBIAN EL NUMERO O LOS ATRIBUTOS DE LA BD
            //********************************************************
            for(int i=1; i<=14; i++){
                //No se ponde hasta que no este la BD hecha bien, en cada uno de los switch
                //sabiendo el numero de columna que es cada datos llamariamos a alerts(idEstacion, columna, datoConsulta)
                //siendo columna un string que nos dice como se llama la columna (temperatura, humedad...)
                String alerta = "";
                switch (i){
                    case 3:
                        alerta = alerts(idEstacion, "temperatura", consulta.getString(i));
                        if(!alerta.isEmpty()){
                            resultado += "  ◾"+ alerta +"\n";
                        }
                        break;
                    case 4:
                        break;
                    case 5:
                        break;
                    default:
                        break;
                }
                
            }
        }
        
        return resultado;
    }
    
    public String refreshTable(int idEstacion) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select ID_Estacion, Latitud, Longitud, Temperatura, Humedad, "
                + "Presion_Atmosferica from datos_recabados t1 inner join estacion t2 on t1.ID_Estacion = t2.ID "
                + "where t1.ID_Estacion = "+ idEstacion+" limit 1");
        
        String resultado = "";
        
        while(consulta.next()){
            //********************************************************
            //CAMBIAR SI SE CAMBIAN EL NUMERO O LOS ATRIBUTOS DE LA BD
            //********************************************************
            for(int i=1; i<=6; i++){
                if(consulta.getString(i)!=null){
                    if(i==6){
                        resultado += consulta.getString(i);
                    //Esto une la longitud y latitud en un unico token
                    }else if(i==2){
                        resultado += consulta.getString(i) + "-";
                    }else{
                        resultado += consulta.getString(i) + "//";
                    }
                }else{
                    if(i==6){
                        resultado += "NULL";
                    }else if(i==2){
                        resultado += "NULL-";
                    }else{
                        resultado += "NULL//";
                    }
                }
            }
            
            resultado += "\n";
        }
        
        return resultado;
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
    
    public String refreshDatos(int idEstacion) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select * from datos_recabados where ID_Estacion="+ idEstacion +" limit "+ 1); //Cambiar segun la base de datos
        
        String resultado = "";
        while(consulta.next()){
            //********************************************************
            //CAMBIAR SI SE CAMBIAN EL NUMERO O LOS ATRIBUTOS DE LA BD
            //********************************************************
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
    
    public String refreshDatosAll(int idEstacion) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select * from datos_recabados where ID_Estacion="+idEstacion+" limit "+ 1); //Cambiar segun la base de datos
        
        String resultado = "";
        while(consulta.next()){
            //********************************************************
            //CAMBIAR SI SE CAMBIAN EL NUMERO O LOS ATRIBUTOS DE LA BD
            //********************************************************
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
