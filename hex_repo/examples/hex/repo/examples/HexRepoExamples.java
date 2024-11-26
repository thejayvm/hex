package hex.repo.examples;

import hex.ql.Query;
import hex.repo.test.Person;
import hex.repo.test.PersonRepository;
import hex.repo.Repository;
import hex.repo.RepositoryBase;
import hex.repo.sql.test.Book;

import static hex.ql.QueryLanguage.*;

public class HexRepoExamples {
    public static void main(String[] args) {
        basicQuery();

        bulkUpdate();
    }

    private static void basicQuery() {
        Repository<Person> people = new PersonRepository();

        from(people).where(Person::getLastName, is("Newton"))
                .forEach((p) -> System.out.println(p.getFullName()));
    }

    private static void bulkUpdate() {
        Repository<Book> repo = new RepositoryBase<>(Book.class);

        Query<Book> books = from(repo).where(Book::getPublishedYear, is(2114));
        repo.update(books, b -> b.setPublishedYear(2014));
    }
}
