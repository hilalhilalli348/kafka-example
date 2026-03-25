package ourcorp.kafkademo.dao.repo;


import org.springframework.data.repository.CrudRepository;
import ourcorp.kafkademo.dao.entity.OutboxEntity;

public interface OutboxRepository extends CrudRepository<OutboxEntity, Long> {
}
