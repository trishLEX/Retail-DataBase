package GUI;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class GUI extends Application {
    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("RetailDB");
        primaryStage.setWidth(1080);
        primaryStage.setHeight(720);

        Tab shop = new Tab("Shops");
        shop.setClosable(false);
        Tab card = new Tab("Cards");
        card.setClosable(false);
        TabPane tabPane = new TabPane(shop, card);

        defaultWindow(shop);
        Scene scene = new Scene(tabPane);
        primaryStage.setScene(scene);
        primaryStage.setResizable(false);
        primaryStage.setFullScreen(false);
        primaryStage.show();
    }

    private void chooseDateWindow(Tab cur) {
        ObservableList<String> weeks = FXCollections.observableArrayList("1", "2", "3", "Not specified");
        ObservableList<String> months = FXCollections.observableArrayList("1", "2", "3", "Not specified");
        ObservableList<Integer> years = FXCollections.observableArrayList(2017, 2018);

        ComboBox<String> weekBox = new ComboBox<>(weeks);
        weekBox.setPromptText("Choose a week");
        weekBox.setPrefWidth(150);
        weekBox.setTranslateY(15);
        weekBox.setTranslateX(15);
        ComboBox<String> monthsBox = new ComboBox<>(months);
        monthsBox.setPromptText("Choose a month");
        monthsBox.setPrefWidth(150);
        monthsBox.setTranslateY(25);
        monthsBox.setTranslateX(15);
        ComboBox<Integer> yearsBox = new ComboBox<>(years);
        yearsBox.setPromptText("Choose a year");
        yearsBox.setPrefWidth(150);
        yearsBox.setTranslateY(35);
        yearsBox.setTranslateX(15);

        Button back = new Button("Back");
        back.setTranslateX(15);
        back.setTranslateY(540);
        back.setOnAction(event -> {
            defaultWindow(cur);
        });

        Button showStats = new Button("Show stats");
        showStats.setTranslateX(800);
        showStats.setTranslateY(15);
        showStats.setPrefWidth(100);
        showStats.setOnAction(event -> {
            showStatsWindow(cur);
        });

        Button diag = new Button("Show diagrams");
        diag.setTranslateX(800);
        diag.setTranslateY(25);
        diag.setPrefWidth(100);
        diag.setOnAction(event -> {
            //TODO
        });

        VBox vBox2 = new VBox(showStats, diag);

        VBox vBox1 = new VBox(weekBox, monthsBox, yearsBox, back);
        HBox hBox = new HBox(vBox1, vBox2);
        cur.setContent(hBox);
    }

    private void showStatsWindow(Tab cur) {

    }

    private void defaultWindow(Tab cur) {
        RadioButton checkAShop = new RadioButton("Check a shop");
        checkAShop.setTranslateX(15);
        checkAShop.setTranslateY(15);

        RadioButton commonStats = new RadioButton("Common stats");
        commonStats.setTranslateX(15);
        commonStats.setTranslateY(50);

        ObservableList<String> shops = FXCollections.observableArrayList("Moscow 1", "Moscow 2");

        ComboBox<String> comboBox = new ComboBox<>(shops);
        comboBox.setPromptText("Choose a Shop");
        comboBox.setTranslateX(50);
        comboBox.setTranslateY(15);
        comboBox.setDisable(true);

        checkAShop.setOnAction(event -> {
            comboBox.setDisable(!comboBox.isDisabled());
        });

        commonStats.setOnAction(event -> {
            comboBox.setDisable(true);
        });

        comboBox.setOnAction(event -> {
            chooseDateWindow(cur);
        });

        ToggleGroup toggleGroup = new ToggleGroup();
        checkAShop.setToggleGroup(toggleGroup);
        commonStats.setToggleGroup(toggleGroup);

        GridPane gridPane = new GridPane();
        gridPane.add(checkAShop, 0 ,0);
        gridPane.add(commonStats, 0, 1);
        gridPane.add(comboBox, 1, 0);

        cur.setContent(gridPane);
    }

    public static void main(String[] args) {
        launch();
    }
}
