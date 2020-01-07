import java.io.*;
import java.net.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class ServidorHilo extends Thread {

    private Socket conexion;

    private DataOutputStream salida;
    private DataInputStream entrada;

    private Connection conexionBD;
    
    private Date fechaActual;
    
    private DateFormat formatoHora, formatoFecha;
    
    private String dominio, usuario, password;
    
    private int aa, mm, dd, hh, mn, ss;

    public ServidorHilo(Socket socket) {

        this.conexion = socket;
        this.dominio = "jdbc:mysql://localhost:3306/estacion_meteorologica_inteligente";
        this.usuario = "root";
        this.password = "WeatherStationUbicua2019";
        this.fechaActual = new Date();
        
        this.formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        this.formatoHora = new SimpleDateFormat("HH:mm:ss");
        
        Calendar fecha = Calendar.getInstance();
        this.aa = fecha.get(Calendar.YEAR);
        this.mm = fecha.get(Calendar.MONTH) + 1;
        this.dd = fecha.get(Calendar.DAY_OF_MONTH);
        this.hh = fecha.get(Calendar.HOUR_OF_DAY);
        this.mn = fecha.get(Calendar.MINUTE);
        this.ss = fecha.get(Calendar.SECOND);
        
        try {

            salida = new DataOutputStream(conexion.getOutputStream());
            entrada = new DataInputStream(conexion.getInputStream());

            Class.forName("com.mysql.jdbc.Driver");

        } catch (IOException ex) {

            System.out.println("ErrorIO: " + ex.getMessage());
        } catch (ClassNotFoundException ex) {

            System.out.println("ErrorCNF: " + ex.getMessage());
        }
        
    }

    public void desconectar() {
        try {

            conexion.close();

        } catch (IOException ex) {

            System.out.println("ErrorIO: " + ex.getMessage());
        }
    }

    @Override
    public void run() {

        String parametros = "";

        try {

            parametros = entrada.readUTF();

            String[] tokens = parametros.split("-");

            switch (tokens.length) {
                case 3:
                    System.out.println(tokens[0] + "-" + tokens[1] + "-" + tokens[2]);
                    break;
                case 2:
                    System.out.println(tokens[0] + "-" + tokens[1]);
                    break;
                default:
                    System.out.println(tokens[0]);
                    break;
            }

            // Refresh-idEstacion
            // Hace una consulta para actualizar los datos mas recientes sobre una estacion
            if (tokens[0].equals("Refresh")) {

                try {
                    conectarBD();
                    String mensaje = refreshDatos(Integer.parseInt(tokens[1]));
                    System.out.println(mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: " + ex.getMessage());
                }
                // HistoricHigh-atributo
                // Devuelve el dato más alto registrado en todas las estaciones sobre un campo,
                // ej: temperatura
            } else if (tokens[0].equals("HistoricHigh")) {
                try {
                    conectarBD();
                    String mensaje = historicInformationHigh(tokens[1]);
                    System.out.println("Dato de " + tokens[1] + " más alto registrado " + mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: " + ex.getMessage());
                }
                // HistoricLow-atributo
                // Devuelve el dato más bajo registrado en todas las estaciones sobre un campo,
                // ej: temperatura
            } else if (tokens[0].equals("HistoricLow")) {
                try {
                    conectarBD();
                    String mensaje = historicInformationLow(tokens[1]);
                    System.out.println("Dato de " + tokens[1] + " más bajo registrado " + mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: " + ex.getMessage());
                }
             
                // RefreshTable-numeroEstaciones teniendo en cuenta que los id de las estaciones
                // van de 1 a n
                // Hace una consulta para refrecar los datos de la tabla (id, ubicacion,
                // temperatura, humedad, presion)
            } else if (tokens[0].equals("RefreshTable")) {
                try {
                    conectarBD();
                    String mensaje = "";
                    // El segundo parametro es el numero de estaciones que queremos
                    for (int i = 1; i <= Integer.parseInt(tokens[1]); i++) {
                        mensaje += refreshTable(i);
                    }
                    System.out.println(mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: " + ex.getMessage());
                }
                // Notify-idEstacion
                // Devuelve un String con alertas sobre los datos de una estacion. Si retorna
                // NULL quiere decir que no hay ningun aviso si en
                // cambio tiene otro contenido se debe mostrar la notificacion
            } else if (tokens[0].equals("Notify")) {
                try {
                    conectarBD();
                    String mensaje = "";
                    mensaje += notifyAlert(Integer.parseInt(tokens[1]));
                    if (mensaje.isEmpty()) {
                        System.out.println("No hay alertas");
                    } else {
                        System.out.println(mensaje);
                    }
                    System.out.println("");
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL1: " + ex.getMessage());
                }
                //NotifyAll
                // Devuelve un String con alertas sobre los datos de todas las estaciones. Si retorna
                // NULL quiere decir que no hay ningun aviso si en
                // cambio tiene otro contenido se debe mostrar la notificacion
            }else if(tokens[0].equals("NotifyAll")){
                try {
                    conectarBD();
                    
                    String mensaje = "";
                    for(int i=1; i<=Integer.parseInt(stationsNumber()); i++){
                        String alerta = notifyAlert(i);
                        if (!alerta.isEmpty()) {
                            mensaje += "ESTACION "+ i +": \n"+ alerta + "\n";
                        }
                    }
                    
                    if (mensaje.isEmpty()) {
                        System.out.println("No hay alertas");
                    } else {
                        System.out.println(mensaje);
                    }
                    
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL1: " + ex.getMessage());
                }
                // Stations-nada devuelve el numero de estaciones registradas en la base de
                // datos
            } else if (tokens[0].equals("Stations")) {
                try {
                    conectarBD();
                    String mensaje = "";
                    mensaje += stationsNumber();
                    System.out.println("Hay " + mensaje + " estaciones conectadas");
                    salida.writeUTF(mensaje);
                    desconectarBD();
                } catch (SQLException ex) {
                    System.out.println("ErrorSQL: " + ex.getMessage());
                }
                //Weather-valorLuz-valorLluvia devuelve la imagen que hay que poner en el TextView Tiempo
            }else if(tokens[0].equals("Weather")){
                try{
                    conectarBD();
                    String mensaje = "";
                    mensaje += weatherImage(tokens[1], tokens[2]);
                    System.out.println("Imagen: "+ mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                }catch(SQLException ex){
                    System.out.println("ErrorSQL: " + ex.getMessage());
                }
                //Graph-idEstacion-columna especificando la columna y el id de la estaciojn nos devielve los datos de las 20 horas
                //anteriores
            } else if(tokens[0].equals("Graph")){
                try{
                    conectarBD();
                    
                    String mensaje = "";
                    
                    String anno = ""+this.aa;
                    String mes = ""+this.mm;
                    String dia = ""+this.dd;
                    String hora = ""+this.hh;
                    String minuto = ""+this.mn;
                    String segundo = ""+this.ss;
                    
                    if(this.mm<10){
                        mes = "0"+this.mm;
                    }
                    if(this.dd<10){
                        dia = "0"+this.dd;
                    }
                    if(this.mn<10){
                        minuto = "0"+this.mn;
                    }
                    
                    int horaInt = Integer.parseInt(hora);
                    
                    for(int i=0; i<20; i++){
                        //Hora en funcion de si pasa de las 00 o no
                        int horaConsulta;
                        int diaConsulta = Integer.parseInt(dia);
                        
                        if(i>horaInt){
                            horaConsulta = 24-(i-horaInt);
                            diaConsulta --;
                        }else{
                            horaConsulta = horaInt-i;
                        }
                        
                        String horaCon = ""+horaConsulta;
                        String diaCon = ""+diaConsulta;
                        
                        //Ponemos bien el formato de la hora, dia y mes para que tenga un 0 delante
                        if(horaConsulta<10){
                            horaCon = "0"+horaConsulta;
                        }
                        if(diaConsulta<10){
                            diaCon = "0"+diaConsulta;
                        }
                        
                        //String fechaConsulta = anno +"-"+ mes +"-"+ dia +" "+ horaCon +":%:%";
                        String fechaConsulta = "%-%-"+ diaCon +" "+ horaCon +":%:%";
                        
                        //Si el de tiempo llamamos a weather
                        if(tokens[2].toLowerCase().equals("tiempo")){
                            mensaje = diaCon+"-"+ horaCon +"h//"+ graphWeather(tokens[1], fechaConsulta) +"\n"+ mensaje;
                        }else{
                            mensaje = diaCon+"-"+ horaCon +"h//"+ graphStation(tokens[1], tokens[2], fechaConsulta) +"\n"+ mensaje;
                        }
                        
                    }
                    
                    System.out.println(mensaje);
                    salida.writeUTF(mensaje);
                    desconectarBD();
                }catch(SQLException ex){
                    System.out.println("ErrorSQL: " + ex.getMessage());
                }
            } 

        } catch (IOException ex) {

            System.out.println("ErrorIO: " + ex.getMessage());
        }

        desconectar();
    }

    public void conectarBD() throws SQLException {

        conexionBD = DriverManager.getConnection(dominio, usuario, password);
    }

    public void desconectarBD() throws SQLException {

        conexionBD.close();
    }
    
    public String graphWeather(String id, String hora) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select Nivel_Luz, Cantidad_Lluvia from datos_recabados where ID_Estacion = "+ id 
                +" and Fecha_Hora LIKE \""+ hora +"\" order by Fecha_Hora desc limit 1");
        
        String consultaRes = "NULL-NULL";
        String resultado = "";
        
        while(consulta.next()){
            consultaRes = "";
            for(int i=1; i<=2; i++){
                if(i==2){
                    consultaRes += "-";
                }
                if(consulta.getString(i)==null){
                    consultaRes += "NULL";
                }else{
                    consultaRes += consulta.getString(i);
                }
            }
        }
        
        String[] tokens = consultaRes.split("-");
        
        resultado += weatherImage(tokens[0], tokens[1]);
        
        if(resultado.isEmpty()){
            resultado += "null.png";
        }
        
        return resultado;
    }
    
    public String graphStation(String id, String columna, String hora) throws SQLException{
        
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select "+ columna +" from datos_recabados where ID_Estacion = "+ id 
                +" and Fecha_Hora LIKE \""+ hora +"\" order by Fecha_Hora desc limit 1");
        
        //select Temperatura from datos_recabados where ID_Estacion = 4 and Fecha_Hora LIKE "%-%-% 16:%:%" ORDER by Fecha_Hora DESC LIMIT 1
        String resultado = "NULL";
        
        while(consulta.next()){
            resultado = consulta.getString(1);
        }
        
        return resultado;
    }

    public String stationsNumber() throws SQLException {

        Statement estado = conexionBD.createStatement();
        // Tambien podria ser la consulta "select count(ID_Estacion) from
        // datos_recabados group by ID_Estacion"
        ResultSet consulta = estado.executeQuery("select count(ID) from estacion");

        String resultado = "";

        while (consulta.next()) {
            resultado = consulta.getString(1);
        }

        return resultado;
    }
    
    public String numberColumns() throws SQLException{
        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select count(*) from information_schema.columns where table_name = 'datos_recabados'");
        
        String resultado = "";
        
        while(consulta.next()){
            resultado += consulta.getString(1);
        }
        
        return resultado;
    }

    public String weatherImage(String luz, String lluvia) {

        String resultado = "";
        
        if(luz.toLowerCase().equals("soleado")){
            if(lluvia.toLowerCase().startsWith("lluvia")){
                resultado += "diaLluvioso.png";
            }else{
                resultado += "soleado.png";
            }
        }else if(luz.toLowerCase().equals("nublado")){
            resultado += "nublado.png";
        }else if(luz.toLowerCase().equals("noche")){
            if(lluvia.toLowerCase().startsWith("lluvia")){
                resultado += "nocheLluviosa.png";
            }else{
                resultado += "noche.png";
            }
        }

        return resultado;
    }

    public String alerts(int idEstacion, String columna, String valor) throws SQLException {

        String resultado = "";
        
        float dato = 0;
        float mayor = 6666, menor = -6666;
        
        //Para que no convierta el valor de la calidad del aire a un float ni que busque 
        //su valor historico maximo o minimo
        if(!valor.isEmpty()){
            if(!columna.equals("calidad_aire") && !columna.equals("nivel_radiacion")){
                
                dato = (float) Float.parseFloat(valor);
                
                String mayorS = historicInformationHigh(columna);
                String menorS = historicInformationLow(columna);
                
                if(!mayorS.isEmpty()){
                    mayor = (float) Float.parseFloat(mayorS);
                }
                if(!menorS.isEmpty()){
                    menor = (float) Float.parseFloat(menorS);
                }
            }

            switch (columna) {
                case "nivel_radiacion":
                    // Avisos de temperatura que supera o es menor que x valor
                    float uva = (float) Float.parseFloat(valor);

                    if (uva >= 6) {
                        resultado += "Se recomienda protección solar: "+ uva +"UV.\n";
                    } else if (uva <= 2) {
                        resultado += "Baja radiación solar: " + uva +"UV.\n";
                    }

                    break;
                case "temperatura":
                    // Avisos de temperatura que supera o es menor que x valor
                    if (dato >= 33) {
                        resultado += "Alta temperatura: " + dato + "ºC. ";
                    } else if (dato <= 5) {
                        resultado += "Baja temperatura: " + dato + "ºC. ";
                    }

                    // Avisos de maximo o minimos historicos en temperatura
                    if (dato >= mayor) {
                        resultado += "Se ha registrado el máximo valor de temperatura.\n";
                    } else if (dato <= menor) {
                        resultado += "Se ha registrado el mínimo de valor temperatura.\n";
                    }else{
                        if(!resultado.isEmpty()){
                            resultado += "\n";
                        }
                    }

                    break;
                case "humedad":
                    // Avisos de humedad que supera o es menor que x valor
                    if(dato >= 65){
                        resultado += "Alta humedad: " + dato + "%. ";
                    }else if(dato<=10){
                        resultado += "Baja humedad: " + dato + "%. ";
                    }

                    // Avisos de maximo o minimos historicos en humedad
                    if (dato >= mayor) {
                        resultado += "Se ha registrado el máximo valor de humedad.\n";
                    } else if (dato <= menor) {
                        resultado += "Se ha registrado el mínimo valor de humedad.\n";
                    }else{
                        if(!resultado.isEmpty()){
                            resultado += "\n";
                        }
                    }

                    break;
                case "calidad_aire":
                    if((valor.equals("pesima"))||(valor.equals("baja"))){
                        resultado += "!Cuidado¡ La calidad del aire es "+ valor + " hoy.\n";
                    }
                default:
                    break;
            }
        }
        
        return resultado;
    }

    public String notifyAlert(int idEstacion) throws SQLException {

        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado
                .executeQuery("select * from datos_recabados where ID_Estacion=" + idEstacion + " order by fecha_hora desc limit " + 1);

        // Cuando el resultado enviado a la app sea NULL querra decir que no hay ningun
        // avsido que mostrar
        String resultado = "";
        int columnas = Integer.parseInt(numberColumns());

        while (consulta.next()) {
            for (int i = 1; i <= columnas; i++) {
                String alerta;
                switch (i) {
                    //temperatura
                    case 3:
                        if(consulta.getString(i)!=null){
                            alerta = alerts(idEstacion, "temperatura", consulta.getString(i));
                            if (!alerta.isEmpty()) {
                                resultado += "  -" + alerta;
                            }
                        }

                        break;
                    //humedad
                    case 4:
                        if(consulta.getString(i)!=null){
                            alerta = alerts(idEstacion, "humedad", consulta.getString(i));
                            if (!alerta.isEmpty()) {
                                resultado += "  -" + alerta;
                            }
                        }

                        break;
                    //radiacion
                    case 8:
                        if(consulta.getString(i)!=null){
                            alerta = alerts(idEstacion, "nivel_radiacion", consulta.getString(i));
                            if (!alerta.isEmpty()) {
                                resultado += "  -" + alerta;
                            }
                        }

                        break;
                    //calidad del aire
                    case 11:
                        if(consulta.getString(i)!=null){
                            alerta = alerts(idEstacion, "calidad_aire", consulta.getString(i));
                            if (!alerta.isEmpty()) {
                                resultado += "  -" + alerta;
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        return resultado;
    }

    public String refreshTable(int idEstacion) throws SQLException {

        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select ID_Estacion, Latitud, Longitud, Temperatura, Humedad, "
                + "Presion_Atmosferica from datos_recabados t1 inner join estacion t2 on t1.ID_Estacion = t2.ID "
                + "where t1.ID_Estacion = " + idEstacion + " order by fecha_hora desc limit 1");

        String resultado = "";

        while (consulta.next()) {
            for (int i = 1; i <= 6; i++) {
                if (consulta.getString(i) != null) {
                    switch (i) {
                        case 6: // Esto une la longitud y latitud en un unico token
                            resultado += consulta.getString(i);
                            break;
                        case 2:
                            resultado += consulta.getString(i) + "-";
                            break;
                        default:
                            resultado += consulta.getString(i) + "//";
                            break;
                    }
                } else {
                    switch (i) {
                        case 6: // Esto une la longitud y latitud en un unico token
                            resultado += "NULL";
                            break;
                        case 2:
                            resultado += "NULL-";
                            break;
                        default:
                            resultado += "NULL//";
                            break;
                    }
                }
            }

            resultado += "\n";
        }

        return resultado;
    }

    public String historicInformationHigh(String dato) throws SQLException {

        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select MAX(" + dato + ") from datos_recabados");

        String resultado = "";

        while (consulta.next()) {
            resultado = consulta.getString(1);
        }

        return resultado;
    }

    public String historicInformationLow(String dato) throws SQLException {

        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select MIN(" + dato + ") from datos_recabados");

        String resultado = "";

        while (consulta.next()) {
            resultado = consulta.getString(1);
        }

        return resultado;
    }

    public String refreshDatos(int idEstacion) throws SQLException {

        Statement estado = conexionBD.createStatement();
        ResultSet consulta = estado.executeQuery("select * from datos_recabados where ID_Estacion=" + idEstacion + " order by fecha_hora desc limit " + 1);

        String resultado = "";
        int columnas = Integer.parseInt(numberColumns());
        
        while (consulta.next()) {
            for (int i = 1; i <= columnas; i++) {
                // Los ifs de dentro hacen que la ultima columna de la consulta no tenga el
                // caracter separador
                if (consulta.getString(i) != null) {
                    if (i == columnas) {
                        resultado += consulta.getString(i);
                    } else {
                        resultado += consulta.getString(i) + "//";
                    }
                } else {
                    if (i == columnas) {
                        resultado += "NULL";
                    } else {
                        resultado += "NULL//";
                    }
                }
            }

            resultado += "\n";
        }

        return resultado;
    }
}
