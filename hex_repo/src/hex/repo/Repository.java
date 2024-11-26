package hex.repo;

import hex.ql.Query;
import hex.ql.Queryable;
import hex.repo.streams.RepositoryStream;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Created by jason on 14-11-01.
 */
public interface Repository<T> extends Queryable<T> {
    Optional<T> find(int id);

    RepositoryStream<T> stream();

    void update(Query<T> queryToUpdate, Consumer<T> updateDescriptor);

    void update(T t);
}
