package ourcorp.kafkademo.dao.repo;

import org.springframework.data.repository.CrudRepository;
import ourcorp.kafkademo.dao.entity.UserEntity;

public interface UserRepository extends CrudRepository<UserEntity, Long> {
}
