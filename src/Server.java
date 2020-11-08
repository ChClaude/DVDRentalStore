import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.SecureRandom;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Date;

import static java.time.temporal.ChronoUnit.DAYS;

/**
 * Server.java
 * @author Lulamela Mfenyana
 * student number: 208097104
 */
public class Server {

    private ObjectInputStream inputStream;
    private ObjectOutputStream outStream;

    private Connection conn = null;
    private Statement stmt = null;
    private final DbInitializer dbInitializer;

    public Server() {
        dbInitializer = new DbInitializer();
    }


    public static void main(String[] args) {
        Server server = new Server();

        server.createDbConnection();
        server.dbInitializer.initialize();

        server.initiateServerConnection();
        server.receiveData();

    }

    public void initiateServerConnection() {
        try{
            System.out.println("Server initiated waiting on port " + 5559);
            Socket client = new ServerSocket(5559).accept();

            outStream = new ObjectOutputStream(client.getOutputStream());
            inputStream = new ObjectInputStream(client.getInputStream());
        }catch (IOException ex){
            ex.printStackTrace();
        }
    }

    public void receiveData() {
        try {
            while (true) {
                String in = (String) inputStream.readObject();

                switch (in.toLowerCase()) {
                    case "list movies for category" -> {
                        String category = (String) inputStream.readObject();
                        listMovies(category);
                    }
                    case "add customer" -> addCustomer();
                    case "add dvd" -> addDvd();
                    case "rent dvd" -> rentDbd();
                    case "return dvd" -> returnDvd();
                    case "list movies" -> listMovies(null);
                    case "list customers" -> listCustomers();
                    case "list rentals" -> listRentals("all");
                    case "list outstanding rentals" -> listRentals("outstanding");
                    case "search movies" -> searchMovie();
                    default -> listRentals(in); // this the case where the in value will contain a slash /
                }
            }
        }catch(IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void returnDvd() {
        try {
            Rental rental = (Rental) inputStream.readObject();
            stmt.executeUpdate("update CUSTOMER set CANRENT=true where CUSTNUMBER=" + rental.getCustNumber());
            stmt.executeUpdate("update DVD set AVAILABLEFORRENT=true where DVDNUMBER=" + rental.getDvdNumber());

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            LocalDateTime now = LocalDateTime.now();

            DateFormat format = new SimpleDateFormat("yyyy/MM/dd");
            Date rentalDate = format.parse(rental.getDateRented());

            long timeElapsed = DAYS.between(rentalDate.toInstant(), new Date().toInstant());

            double totalCostPenalty;

            if (timeElapsed > 1)
                totalCostPenalty = 0;
            else
                totalCostPenalty = (timeElapsed - 1) * 5.0;

            stmt.executeUpdate("update RENTAL set DATERETURNED='" + dtf.format(now) + "', TOTALPENALTYCOST=" + totalCostPenalty
                    + " where RENTALNUMBER=" + rental.getRentalNumber());

        } catch (IOException | ClassNotFoundException | SQLException | ParseException e) {
            e.printStackTrace();
        }
    }

    private void rentDbd() {
        try {
            String custNumber = ((String) inputStream.readObject()).trim();
            String dvdNumber =  ((String) inputStream.readObject()).trim();

            stmt.executeUpdate("update DVD set AVAILABLEFORRENT=false where DVDNUMBER=" + dvdNumber);
            stmt.executeUpdate("update CUSTOMER set CANRENT=false where CUSTNUMBER=" + custNumber);

            PreparedStatement statement = conn.prepareStatement("insert into rental values (?, ?, ?, ?, ?, ?)");
            int randomRentalNum = Math.abs(new SecureRandom().nextInt(97845641));

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            LocalDateTime now = LocalDateTime.now();

            statement.setInt(1, randomRentalNum);
            statement.setString(2, dtf.format(now));
            statement.setString(3, "NA");
            statement.setDouble(4, 0);
            statement.setInt(5, Integer.parseInt(custNumber));
            statement.setInt(6, Integer.parseInt(dvdNumber));

            statement.executeUpdate();

            outStream.writeObject("Rental operation done with success!!!");
            outStream.flush();

        } catch (IOException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

    }


    private void searchMovie() {
        try {
            String in = (String) inputStream.readObject();

            ResultSet set = stmt.executeQuery("select * from DVD where upper(TITLE) like '%" + in.toUpperCase() + "%'");
            getDvdList(set);
        } catch (IOException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void listRentals(String option) {
        try{
            ResultSet set;
            if (option.equalsIgnoreCase("outstanding")) {
                set = stmt.executeQuery("SELECT * FROM RENTAL WHERE DATERETURNED='NA'");
            } else if(option.contains("/")) {
                set = stmt.executeQuery("SELECT * FROM RENTAL WHERE DATERENTED='" + option + "'");
            }else {
                set = stmt.executeQuery("SELECT * FROM RENTAL");
            }

            List<Rental> rentalList = new ArrayList<>();

            while (set.next()){
                int rentalNumber= set.getInt("rentalNumber");
                String dateRented = set.getString("dateRented");
                String dateReturned = set.getString("dateReturned");
                int custNumber = set.getInt("custNumber");
                int dvdNumber = set.getInt("dvdNumber");

                Rental rental = new Rental(rentalNumber, dateRented, dateReturned, custNumber, dvdNumber);
                rentalList.add(rental);
            }

            rentalList.sort(Comparator.comparing(Rental::getDateRented));

            outStream.writeObject(rentalList);
            outStream.flush();
        }catch (SQLException | IOException e){
            e.printStackTrace();
        }
    }

    private void listMovies(String categorySearch) {
        try{

            ResultSet set;

            if(categorySearch != null) {
                set = stmt.executeQuery("select * from DVD where upper(CATEGORY) like '%" +
                        categorySearch.toUpperCase() + "%' AND AVAILABLEFORRENT = true");
            } else {
                set = stmt.executeQuery("SELECT * FROM DVD");
            }

            getDvdList(set);
        }catch (SQLException | IOException e){
            e.printStackTrace();
        }
    }

    private void getDvdList(ResultSet set) throws SQLException, IOException {
        List<DVD> dvdList = new ArrayList<>();

        while (set.next()){
            int dvdNumber= set.getInt("dvdNumber");
            String title = set.getString("title");
            String category = set.getString("category");
            boolean newRelease = set.getBoolean("newRelease");
            boolean availableForRent = set.getBoolean("availableForRent");

            DVD dvd = new DVD(dvdNumber, title, category, newRelease, availableForRent);
            dvdList.add(dvd);
        }
        dvdList.sort(Comparator.comparing(DVD::getCategory));

        outStream.writeObject(dvdList);
        outStream.flush();
    }

    private void listCustomers() {
        try{
            ResultSet set = stmt.executeQuery("SELECT * FROM CUSTOMER");
            List<Customer> customerList = new ArrayList<>();

            while (set.next()){
                int customerNumber= set.getInt("custNumber");
                String firstName = set.getString("firstName");
                String surname = set.getString("surname");
                String phoneNum = set.getString("phoneNum");
                double credit = set.getDouble("credit");
                boolean canRent = set.getBoolean("canRent");

                Customer customer = new Customer(customerNumber, firstName, surname, phoneNum, credit, canRent);
                customerList.add(customer);
            }

            customerList.sort(Comparator.comparing(Customer::getName));

            outStream.writeObject(customerList);
            outStream.flush();
        }catch (SQLException | IOException e){
            e.printStackTrace();
        }
    }

    private void addDvd() {
        try {
            insertDvdIntoDatabase(inputStream);
        } catch (IOException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertDvdIntoDatabase(ObjectInput inputStream) throws IOException, ClassNotFoundException, SQLException {
        DVD dvd = (DVD) inputStream.readObject();

        PreparedStatement statement = conn.prepareStatement("insert into dvd values (?, ?, ?, ?, ?, ?)");
        statement.setInt(1, dvd.getDvdNumber());
        statement.setString(2, dvd.getTitle());
        statement.setString(3, dvd.getCategory());
        statement.setDouble(4, dvd.getPrice());
        statement.setBoolean(5, dvd.isNewRelease());
        statement.setBoolean(6, dvd.isAvailable());

        statement.executeUpdate();
    }

    private void addCustomer() {
        try {
            insertCustomerIntoDatabase(inputStream);
        } catch (IOException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertCustomerIntoDatabase(ObjectInput inputStream) throws IOException, ClassNotFoundException, SQLException {
        Customer customer = (Customer) inputStream.readObject();

        PreparedStatement statement = conn.prepareStatement("insert into customer values (?, ?, ?, ?, ?, ?)");
        statement.setInt(1, customer.getCustNumber());
        statement.setString(2, customer.getName());
        statement.setString(3, customer.getSurname());
        statement.setString(4, customer.getPhoneNum());
        statement.setDouble(5, customer.getCredit());
        statement.setBoolean(6, customer.canRent());

        statement.executeUpdate();
    }

    private void createDbConnection() {
        try {
            System.out.println("Connecting to database...");
            // Database variables
            String JDBC_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
            Class.forName(JDBC_DRIVER);
            String DB_URL = "jdbc:derby:DvDRentalDb;create=true";
            conn = DriverManager.getConnection(DB_URL);
            System.out.println("Connection successful!");
        } catch (Exception except) {
            except.printStackTrace();
        }
    }

    private void close() {
        try {
            System.out.println("Closing database");
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException sqlExcept) {
            System.err.println("An error occurred while closing the database");
            System.err.println(sqlExcept.getMessage());
        }
    }

    private class DbInitializer {

        public void initialize() {
            seedDatabase();
        }

        private void seedDatabase() {
            System.out.println("Seeding database...");
            // TODO check if the table has data if - yes dont drop them - else drop them
            try {
                DatabaseMetaData metadata = conn.getMetaData();
                ResultSet resultSet = metadata.getTables(null, null, null, new String[]{"TABLE"});
                stmt = conn.createStatement();
                List<String> tables = new ArrayList<>();

                while (resultSet.next()) {
                    tables.add(resultSet.getString("TABLE_NAME").toLowerCase());
                }

                if (tables.contains("rental")) {
                    stmt.executeUpdate("alter table rental drop constraint custNumber_fk");
                    stmt.executeUpdate("alter table rental drop constraint dvdNumber_fk");
                    stmt.executeUpdate("drop table rental");
                }

                if (tables.contains("customer")) {
                    stmt.executeUpdate("drop table customer");
                }

                if (tables.contains("dvd")) {
                    stmt.executeUpdate("drop table dvd");
                }

                stmt.executeUpdate("create table customer(custNumber int not null, firstName varchar(40), surname varchar(40)," +
                        "phoneNum varchar(40), credit decimal(10, 2), canRent boolean, primary key (custNumber))");

                stmt.executeUpdate("create table dvd(dvdNumber int not null, title varchar(40), category varchar(40)," +
                        "price double, newRelease boolean, availableForRent boolean, primary key (dvdNumber))");

                // TODO: Make sure all the date inserted has the following format yyyy/mm/dd - there's a date inserted as 2016/9/01 which makes it hard to search
                stmt.executeUpdate("create table rental(rentalNumber int not null, dateRented varchar(40), dateReturned " +
                        "varchar(40), totalPenaltyCost decimal(10, 2), custNumber int, dvdNumber int, primary key (rentalNumber), " +
                        "constraint custNumber_fk FOREIGN KEY (custNumber) REFERENCES customer(custNumber), constraint dvdNumber_fk " +
                        "FOREIGN KEY (dvdNumber) REFERENCES dvd(dvdNumber))");

                readAndInsertData();

                System.out.println("Database successfully seeded!!!");
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        public void readAndInsertData() {
            int num = 0;

            try (
                    InputStream file = new FileInputStream("./src/Customers.ser");
                    InputStream buffer = new BufferedInputStream(file);
                    ObjectInput input = new ObjectInputStream(buffer)
            ) {
                while (true) {
                    //deserialize the List
                    insertCustomerIntoDatabase(input);
                    num++;
                }

            } catch (EOFException ignored) {
                System.out.println(num + " customers inserted");
            } catch (ClassNotFoundException | IOException ex) {
                System.err.println(ex.getMessage());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

            try (
                    InputStream file = new FileInputStream("./src/Movies.ser");
                    InputStream buffer = new BufferedInputStream(file);
                    ObjectInput input = new ObjectInputStream(buffer)
            ) {
                num = 0;
                while (true) {
                    //deserialize the List
                    insertDvdIntoDatabase(input);
                    num++;
                }

            } catch (EOFException ignored) {
                System.out.println(num + " DVDs inserted");
            } catch (ClassNotFoundException | IOException ex) {
                System.err.println(ex.getMessage());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

            try (
                    InputStream file = new FileInputStream("./src/Rental.ser");
                    InputStream buffer = new BufferedInputStream(file);
                    ObjectInput input = new ObjectInputStream(buffer)
            ) {
                num = 0;
                while (true) {
                    //deserialize the List
                    Rental rental = (Rental) input.readObject();

                    PreparedStatement statement = conn.prepareStatement("insert into rental values (?, ?, ?, ?, ?, ?)");
                    statement.setInt(1, rental.getRentalNumber());
                    statement.setString(2, rental.getDateRented());
                    statement.setString(3, rental.getDateReturned());
                    statement.setDouble(4, rental.getTotalPenaltyCost());
                    statement.setInt(5, rental.getCustNumber());
                    statement.setInt(6, rental.getDvdNumber());

                    statement.executeUpdate();
                    num++;
                }

            } catch (EOFException ignored) {
                System.out.println(num + " rentals inserted");
            } catch (ClassNotFoundException | IOException ex) {
                System.err.println(ex.getMessage());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

}
