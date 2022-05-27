import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.driver.*;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.neo4j.driver.Values.parameters;

public class Neo4j implements AutoCloseable {
    //MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r

    private final Driver driver;

    public Neo4j(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void initNeo4j(final String message) {
        try (Session session = driver.session()) {
            String greeting = session.writeTransaction(tx ->
            {
                Result result = tx.run("CREATE (a:Greeting) " +
                                "SET a.message = $message " +
                                "RETURN a.message + ', from node ' + id(a)",
                        parameters("message", message));
                return result.single().get(0).asString();
            });
            System.out.println(greeting);
        }
    }

    //MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r
    public void clearDB() {
        try (Session session = driver.session()) {
            String query = session.writeTransaction(tx ->
            {
                Result result = tx.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r");
                return null;
            });
        }
    }


    public void initTableBank() {
        try (Session session = driver.session()) {
            String query = session.writeTransaction(tx ->
            {
                Result result = tx.run("CREATE (bankSamara:Bank {terbank:'043601607', bankName:'ПОВОЛЖСКИЙ БАНК ПАО СБЕРБАНК', city:'Самара'})," +
                        "(bankMoscow:Bank {terbank:'044525225', bankName:'ПАО СБЕРБАНК', city:'Москва'})," +
                        "(bankEkb:Bank {terbank:'046577674', bankName:'Уральский ПАО Сбербанк', city:'Екатеринбург'})");
                return null;
            });
        }
    }

    public void initTableCurrency() {
        try (Session session = driver.session()) {
            String query = session.writeTransaction(tx ->
            {
                Result result = tx.run("CREATE (currencyRUB:Currency {id:'1', currencyName:'RUB'})," +
                        "(currencyUSD:Currency {id:'2', currencyName:'USD'})," +
                        "(currencyEUR:Currency {id:'2', currencyName:'EUR'})");
                return null;
            });
        }
    }

    public void initTableAccountStatus() {
        try (Session session = driver.session()) {
            String query = session.writeTransaction(tx ->
            {
                Result result = tx.run("CREATE (accountStatusActive:AccountStatus {id:'1', accountStatusName:'Active'})," +
                        "(accountStatusClosed:AccountStatus {id:'2', accountStatusName:'Closed'})," +
                        "(accountStatusArrested:AccountStatus {id:'3', accountStatusName:'Arrested'})");
                return null;
            });
        }
    }


    public void initTableClient() {
        try (Session session = driver.session()) {
            String query = session.writeTransaction(tx ->
            {
                Result addClientKalinin = tx.run("CREATE (clientKalinin:Client {id:'1', firstName:'Александр', lastName:'Калинин'})," +
                        "(clientVlasov:Client {id:'2', firstName:'Георгий', lastName:'Власов'})," +
                        "(clientKiseleva:Client {id:'3', firstName:'Снежана', lastName:'Киселева'})," +
                        "(clientTyshkun:Client {id:'4', firstName:'Андрей', lastName:'Тышкун'})," +
                        "(clientWithoutAccount1:Client {id:'5', firstName:'test', lastName:'test'})," +
                        "(clientWithoutAccount2:Client {id:'6', firstName:'test', lastName:'test'})");
                return null;
            });
        }
    }

    public void initTableCard() {
        try (Session session = driver.session()) {
            String query = session.writeTransaction(tx ->
            {
                Result result = tx.run("CREATE (cardForKalinin:Card {id:'4215532534155314', dateStart:'2020-01-01', dateEnd:'2022-01-01', cvc:'142', cash:'50,0'})," +
                        "(cardForVlasov:Card {id:'5463256874589632', dateStart:'2020-01-01', dateEnd:'2022-01-01', cvc:'415', cash:'1250,0'})," +
                        "(cardForiKseleva:Card {id:'4523698547851236', dateStart:'2020-01-01', dateEnd:'2022-01-01', cvc:'534', cash:'4525650,0'})," +
                        "(cardForTyshkun:Card {id:'9632587412543652', dateStart:'2020-01-01', dateEnd:'2022-01-01', cvc:'526', cash:'32523550,0'})");
                return null;
            });
        }
    }

    public void initTableAccount() {
        try (Session session = driver.session()) {
            String query = session.writeTransaction(tx ->
            {
                Result result = tx.run("CREATE (accountForKalinin:Account {id:'25632145874563225698', balance:'1000,0'})," +
                        "(accountForVlasov:Account {id:'23659874123698563257', balance:'12500,0'})," +
                        "(accountForiKseleva:Account {id:'25489652314587459865', balance:'54367457,0'})," +
                        "(accountForTyshkun:Account {id:'69696585852356214578', balance:'756474673838,0'})");
                return null;
            });
        }
    }

    public void initEntities() {
        try (Session session = driver.session()) {
            String query = session.writeTransaction(tx ->
            {
                Result bankAccount1 = tx.run("MATCH (a:Bank), (b:Account) WHERE a.bankName='ПОВОЛЖСКИЙ БАНК ПАО СБЕРБАНК' AND b.id='25632145874563225698'" +
                        "CREATE (a)-[r: обслуживает]-> (b)");
                Result bankAccount2 = tx.run("MATCH (a:Bank), (b:Account) WHERE a.bankName='ПАО СБЕРБАНК' AND b.id='23659874123698563257'" +
                        "CREATE (a)-[r: обслуживает]-> (b)");
                Result bankAccount3 = tx.run("MATCH (a:Bank), (b:Account) WHERE a.bankName='Уральский ПАО Сбербанк' AND b.id='25489652314587459865'" +
                        "CREATE (a)-[r: обслуживает]-> (b)");
                Result bankAccount4 = tx.run("MATCH (a:Bank), (b:Account) WHERE a.bankName='ПОВОЛЖСКИЙ БАНК ПАО СБЕРБАНК' AND b.id='69696585852356214578'" +
                        "CREATE (a)-[r: обслуживает]-> (b)");

                Result bankAccount11 = tx.run("MATCH (a:Bank), (b:Account) WHERE a.bankName='ПОВОЛЖСКИЙ БАНК ПАО СБЕРБАНК' AND b.id='25632145874563225698'" +
                        "CREATE (b)-[r: принадлежит]-> (a)");
                Result bankAccount22 = tx.run("MATCH (a:Bank), (b:Account) WHERE a.bankName='ПАО СБЕРБАНК' AND b.id='23659874123698563257'" +
                        "CREATE (b)-[r: принадлежит]-> (a)");
                Result bankAccount33 = tx.run("MATCH (a:Bank), (b:Account) WHERE a.bankName='Уральский ПАО Сбербанк' AND b.id='25489652314587459865'" +
                        "CREATE (b)-[r: принадлежит]-> (a)");
                Result bankAccount44 = tx.run("MATCH (a:Bank), (b:Account) WHERE a.bankName='ПОВОЛЖСКИЙ БАНК ПАО СБЕРБАНК' AND b.id='69696585852356214578'" +
                        "CREATE (b)-[r: принадлежит]-> (a)");

                Result currencyAccount1 = tx.run("MATCH (a:Currency), (b:Account) WHERE a.id='1' AND b.id='25632145874563225698'" +
                        "CREATE (b)-[r: имеет]-> (a)");
                Result currencyAccount2 = tx.run("MATCH (a:Currency), (b:Account) WHERE a.id='2' AND b.id='23659874123698563257'" +
                        "CREATE (b)-[r: имеет]-> (a)");
                Result currencyAccount3 = tx.run("MATCH (a:Currency), (b:Account) WHERE a.id='3' AND b.id='25489652314587459865'" +
                        "CREATE (b)-[r: имеет]-> (a)");
                Result currencyAccount4 = tx.run("MATCH (a:Currency), (b:Account) WHERE a.id='1' AND b.id='69696585852356214578'" +
                        "CREATE (b)-[r: имеет]-> (a)");

                Result statusAccount1 = tx.run("MATCH (a:AccountStatus), (b:Account) WHERE a.id='1' AND b.id='25632145874563225698'" +
                        "CREATE (b)-[r: идентифицируется]-> (a)");
                Result statusAccount2 = tx.run("MATCH (a:AccountStatus), (b:Account) WHERE a.id='2' AND b.id='23659874123698563257'" +
                        "CREATE (b)-[r: идентифицируется]-> (a)");
                Result statusAccount3 = tx.run("MATCH (a:AccountStatus), (b:Account) WHERE a.id='3' AND b.id='25489652314587459865'" +
                        "CREATE (b)-[r: идентифицируется]-> (a)");
                Result statusAccount4 = tx.run("MATCH (a:AccountStatus), (b:Account) WHERE a.id='1' AND b.id='69696585852356214578'" +
                        "CREATE (b)-[r: идентифицируется]-> (a)");

                Result clientAccount1 = tx.run("MATCH (a:Client), (b:Account) WHERE a.id='1' AND b.id='25632145874563225698'" +
                        "CREATE (a)-[r: имеет]-> (b)");
                Result clientAccount2 = tx.run("MATCH (a:Client), (b:Account) WHERE a.id='2' AND b.id='23659874123698563257'" +
                        "CREATE (a)-[r: имеет]-> (b)");
                Result clientAccount3 = tx.run("MATCH (a:Client), (b:Account) WHERE a.id='3' AND b.id='25489652314587459865'" +
                        "CREATE (a)-[r: имеет]-> (b)");
                Result clientAccount4 = tx.run("MATCH (a:Client), (b:Account) WHERE a.id='4' AND b.id='69696585852356214578'" +
                        "CREATE (a)-[r: имеет]-> (b)");
                Result clientAccount11 = tx.run("MATCH (a:Client), (b:Account) WHERE a.id='1' AND b.id='25632145874563225698'" +
                        "CREATE (b)-[r: принадлежит]-> (a)");
                Result clientAccount22 = tx.run("MATCH (a:Client), (b:Account) WHERE a.id='2' AND b.id='23659874123698563257'" +
                        "CREATE (b)-[r: принадлежит]-> (a)");
                Result clientAccount33 = tx.run("MATCH (a:Client), (b:Account) WHERE a.id='3' AND b.id='25489652314587459865'" +
                        "CREATE (b)-[r: принадлежит]-> (a)");
                Result clientAccount44 = tx.run("MATCH (a:Client), (b:Account) WHERE a.id='4' AND b.id='69696585852356214578'" +
                        "CREATE (b)-[r: принадлежит]-> (a)");

                Result accountCard1 = tx.run("MATCH (a:Account), (b:Card) WHERE a.id='25632145874563225698' AND b.id='1'" +
                        "CREATE (a)-[r: эксплуатируется]-> (b)");
                Result accountCard2 = tx.run("MATCH (a:Account), (b:Card) WHERE a.id='23659874123698563257' AND b.id='2'" +
                        "CREATE (a)-[r: эксплуатируется]-> (b)");
                Result accountCard3 = tx.run("MATCH (a:Account), (b:Card) WHERE a.id='25489652314587459865' AND b.id='3'" +
                        "CREATE (a)-[r: эксплуатируется]-> (b)");
                Result accountCard4 = tx.run("MATCH (a:Account), (b:Card) WHERE a.id='69696585852356214578' AND b.id='4'" +
                        "CREATE (a)-[r: эксплуатируется]-> (b)");
                Result accountCard11 = tx.run("MATCH (a:Account), (b:Card) WHERE a.id='25632145874563225698' AND b.id='1'" +
                        "CREATE (b)-[r: относится]-> (a)");
                Result accountCard22 = tx.run("MATCH (a:Account), (b:Card) WHERE a.id='23659874123698563257' AND b.id='2'" +
                        "CREATE (b)-[r: относится]-> (a)");
                Result accountCard33 = tx.run("MATCH (a:Account), (b:Card) WHERE a.id='25489652314587459865' AND b.id='3'" +
                        "CREATE (b)-[r: относится]-> (a)");
                Result accountCard44 = tx.run("MATCH (a:Account), (b:Card) WHERE a.id='69696585852356214578' AND b.id='4'" +
                        "CREATE (b)-[r: относится]-> (a)");
                return null;
            });
        }
    }

    public void query() {
        try (Session session = driver.session()) {
            Result result = session.run("MATCH (client:Client)-[:имеет]->(:Account) RETURN client.lastName");
            result.list().forEach(System.out::println);
        }
    }


    public static void main(String... args) throws Exception {
        try (Neo4j greeter = new Neo4j("bolt://localhost:7687", "neo4j", "s3cr3t")) {
            greeter.clearDB();
            greeter.initTableBank();
            greeter.initTableCurrency();
            greeter.initTableCard();
            greeter.initTableClient();
            greeter.initTableAccountStatus();
            greeter.initTableAccount();
            greeter.initEntities();
            greeter.query();
        }
    }
}