package hex.repo;

import hex.ql.Query;
import hex.ql.Queryable;
import hex.ql.ast.InvalidAstException;
import hex.ql.ast.predicates.EqualityPredicate;
import hex.repo.metadata.DataMappingException;
import hex.repo.metadata.Metadata;
import hex.repo.sql.PreparedSqlQuery;
import hex.repo.streams.RepositoryStream;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by jason on 14-11-05.
 */
public abstract class AbstractRepository<T> implements Repository<T>, Queryable<T> {
    public abstract Metadata<T> get_metadata();

    public Optional<T> find(int id) {
        return stream().where(get_metadata().getPrimaryKey(), new EqualityPredicate<>(id)).findFirst();
    }

    public RepositoryStream<T> stream() {
        return new RepositoryStream<>(this);
    }

    public <R> R executeQuery(String sql, ResultSetMapper<R> mapper) {
        try(Connection conn = ConnectionManager.getConnection();
            Statement stmt = conn.createStatement();
            ResultSetWrapper rs = new ResultSetWrapper(stmt.executeQuery(sql))) {
            return mapper.apply(rs);
        } catch (SQLException | DataMappingException e) {
            throw new RepositoryException(e);
        }
    }

    public List<T> executeQuery(String sql) {
        return executeQuery(sql, ResultSetMapper.toList(get_metadata()::mapRecord));
    }

    public <R> R executeQuery(PreparedSqlQuery query, ResultSetMapper<R> mapper) {
        try(Connection conn = ConnectionManager.getConnection();
            PreparedStatementWrapper stmt = new PreparedStatementWrapper(conn.prepareStatement(query.toSql()), query.getParameterValues());
            ResultSetWrapper rs = stmt.executeQuery()) {
            return mapper.apply(rs);
        } catch (SQLException | InvalidAstException | DataMappingException e) {
            throw new RepositoryException(e);
        }
    }

    public List<T> executeQuery(PreparedSqlQuery query) {
        return executeQuery(query, ResultSetMapper.toList(get_metadata()::mapRecord));
    }

    @Override
    public void update(Query<T> queryToUpdate, Consumer<T> updateDescriptor) {
        if(queryToUpdate instanceof RepositoryStream) {
            ((RepositoryStream<T>) queryToUpdate).update(updateDescriptor);
        } else {
            // TODO: Log INFO about performance of this
            queryToUpdate.forEach(updateDescriptor.andThen(this::update));
        }
    }

    @Override
    public void update(T t) {
        Function<T, Integer> primaryKey = get_metadata().getPrimaryKey();
        RepositoryStream<T> stream = (RepositoryStream<T>) stream().where(primaryKey, new EqualityPredicate<>(primaryKey.apply(t)));
        stream.update(t);
    }

    @Override
    public Iterator<T> iterator() {
        return stream().iterator();
    }

    public abstract String getTableName();
}
