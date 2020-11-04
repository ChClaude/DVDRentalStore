import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Server {

    private ObjectInputStream inputStream;
    private ObjectOutputStream outStream;
    private ObjectInputStream inputStreamSer;
    private Socket client;

    // Database variables
    private final String JDBC_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private final String DB_URL = "jdbc:derby:DvDRentalDb;create=true";
    private Connection conn = null;
    private Statement stmt = null;
    private DbInitializer dbInitializer;

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
            client = new ServerSocket(5559).accept();

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
                    /*case "send customers": {
                        returnCustomers();
                        break;
                    }*/
                    case "add customer": {
                        addCustomer();
                        break;
                    }
                    case "add dvd": {
                        addDvd();
                        break;
                    }
                    /*
                    case "rental dvd": {
                        rentDbd();
                        break;
                    }
                    case "return dvd":{
                        returnDvd();
                        break;
                    }*/
                    case "list movies": {
                        listMovies();
                        break;
                    }
                    case "list customers": {
                        listCustomers();
                        break;
                    }
                }
            }
        }catch(IOException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    private void listMovies() {
        try{
            ResultSet set = stmt.executeQuery("SELECT * FROM DVD");
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
        }catch (SQLException | IOException e){
            e.printStackTrace();
        }
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
            DVD dvd = (DVD) inputStream.readObject();

            PreparedStatement statement = conn.prepareStatement("insert into dvd values (?, ?, ?, ?, ?, ?)");
            statement.setInt(1, dvd.getDvdNumber());
            statement.setString(2, dvd.getTitle());
            statement.setString(3, dvd.getCategory());
            statement.setDouble(4, dvd.getPrice());
            statement.setBoolean(5, dvd.isNewRelease());
            statement.setBoolean(6, dvd.isAvailable());

            statement.executeUpdate();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private void addCustomer() {
        try {
            Customer customer = (Customer) inputStream.readObject();

            PreparedStatement statement = conn.prepareStatement("insert into customer values (?, ?, ?, ?, ?, ?)");
            statement.setInt(1, customer.getCustNumber());
            statement.setString(2, customer.getName());
            statement.setString(3, customer.getSurname());
            statement.setString(4, customer.getPhoneNum());
            statement.setDouble(5, customer.getCredit());
            statement.setBoolean(6, customer.canRent());

            statement.executeUpdate();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private void createDbConnection() {
        try {
            System.out.println("Connecting to database...");
            Class.forName(JDBC_DRIVER);
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
                    Customer customer = (Customer) input.readObject();

                    PreparedStatement statement = conn.prepareStatement("insert into customer values (?, ?, ?, ?, ?, ?)");
                    statement.setInt(1, customer.getCustNumber());
                    statement.setString(2, customer.getName());
                    statement.setString(3, customer.getSurname());
                    statement.setString(4, customer.getPhoneNum());
                    statement.setDouble(5, customer.getCredit());
                    statement.setBoolean(6, customer.canRent());

                    statement.executeUpdate();
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
                    DVD dvd = (DVD) input.readObject();

                    PreparedStatement statement = conn.prepareStatement("insert into dvd values (?, ?, ?, ?, ?, ?)");
                    statement.setInt(1, dvd.getDvdNumber());
                    statement.setString(2, dvd.getTitle());
                    statement.setString(3, dvd.getCategory());
                    statement.setDouble(4, dvd.getPrice());
                    statement.setBoolean(5, dvd.isNewRelease());
                    statement.setBoolean(6, dvd.isAvailable());

                    statement.executeUpdate();
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
