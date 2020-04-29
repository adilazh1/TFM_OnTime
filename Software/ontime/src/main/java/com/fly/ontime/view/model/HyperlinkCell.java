package com.fly.ontime.view.model;
 
import javafx.scene.control.Hyperlink;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.util.Callback;
 
public class HyperlinkCell implements  Callback<TableColumn<LayoutFlight, Hyperlink>, TableCell<LayoutFlight, Hyperlink>> {
//	public class HyperlinkCell implements  Callback<TableColumn<LayoutFlight, Hyperlink>, TableCell<LayoutFlight, Hyperlink>> {
 
    @Override
    public TableCell<LayoutFlight, Hyperlink> call(TableColumn<LayoutFlight, Hyperlink> arg) {
        TableCell<LayoutFlight, Hyperlink> cell = new TableCell<LayoutFlight, Hyperlink>() {
            @Override
            protected void updateItem(Hyperlink item, boolean empty) {
            	setGraphic(item);
            }
        };
        return cell;
    }
}