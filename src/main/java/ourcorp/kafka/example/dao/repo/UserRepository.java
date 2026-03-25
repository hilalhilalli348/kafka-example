package ourcorp.kafka.example.dao.repo;

import org.springframework.data.repository.CrudRepository;
import ourcorp.kafka.example.dao.entity.UserEntity;

public interface UserRepository extends CrudRepository<UserEntity, Long> {
}
