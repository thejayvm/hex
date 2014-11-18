package hex.repo;

import hex.repo.streams.RepositoryStream;
import hex.ql.ast.InvalidAstException;

import java.util.List;
import java.util.stream.Collectors;

import static hex.ql.QueryLanguage.*;

/**
 * Created by jason on 14-10-25.
 */
public class HexRepoMain {
    public static void main(String[] args) throws InvalidAstException {
        String sql, expected;
        RepositoryBase<Person> repo = new PersonRepository();
        sql = repo.toSql();
        expected = "SELECT * FROM people";
        if(!expected.equals(sql)) System.exit(-1);

        RepositoryStream<Person> q = (RepositoryStream<Person>) repo.where(Person::getFirstName, is("Jason"));

        sql = q.toSql();
        expected = "SELECT * FROM people WHERE first_name = 'Jason'";
        if(!expected.equals(sql)) System.exit(1);

        q = (RepositoryStream<Person>) q.where(Person::getLastName, is("Wall"));
        sql = q.toSql();
        expected = "SELECT * FROM people WHERE (first_name = 'Jason') AND (last_name = 'Wall')";
        if(!expected.equals(sql)) System.exit(2);

        q = (RepositoryStream<Person>)new RepositoryStream<>(repo).filter(
                field(Person::getLastName, is("Wall"))
                    .and(field(Person::getFirstName, is("Jason")).or(field(Person::getFirstName, is("Natalie"))))
        );

        sql = q.toSql();
        expected = "SELECT * FROM people WHERE (last_name = 'Wall') AND ((first_name = 'Jason') OR (first_name = 'Natalie'))";
        if(!expected.equals(sql)) System.exit(3);

        List<Person> people = q.collect(Collectors.toList());
        if(people.size() != 2) System.exit(5);

        people.forEach((p) -> System.out.printf("%d:%s %s", p.getId(), p.getFirstName(), p.getLastName()).println());

        q = (RepositoryStream<Person>) q.where(Person::getFirstName, is("Bryce"));

        if(q.collect(Collectors.toList()).size() > 0) System.exit(6);

        q.filter((p) -> p.getFirstName().equals("Wayne")).forEach((p) -> System.exit(7));

        Person j = repo.find(1).get();
        if(!j.getFirstName().equals("Jason")) System.exit(8);
    }
}
