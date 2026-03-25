package ourcorp.kafka.example.dao.repo;


import org.springframework.data.repository.CrudRepository;
import ourcorp.kafka.example.dao.entity.OutboxEntity;

public interface OutboxRepository extends CrudRepository<OutboxEntity, Long> {
}
