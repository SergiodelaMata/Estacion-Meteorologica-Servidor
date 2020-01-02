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
public class Main {
    public static void main(String[] args) {
        ClienteNotify notify = new ClienteNotify("hola");
        ClienteRefresh refresh = new ClienteRefresh("hola");
        ClienteRefreshAll refreshAll = new ClienteRefreshAll("hola");
        ClienteRefreshTable refreshTable = new ClienteRefreshTable("hola");
        ClienteStations stations = new ClienteStations("hola");
        refresh.call();
        refreshAll.call();
        refreshTable.call();
        notify.call();
        stations.call();
    }
    
}
