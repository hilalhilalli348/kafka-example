package ourcorp.kafka.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ourcorp.kafka.example.dao.entity.OutboxEntity;
import ourcorp.kafka.example.dao.repo.UserRepository;
import ourcorp.kafka.example.model.request.UserCreatedRequest;
import ourcorp.kafka.example.dao.entity.UserEntity;
import ourcorp.kafka.example.dao.repo.OutboxRepository;

@Slf4j
@RequiredArgsConstructor
@Service
public class UserService {

    private final UserRepository userRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @SneakyThrows
    @Transactional
    public void createUser(UserCreatedRequest request) {

        var userEntity = UserEntity.builder()
                .name(request.getName())
                .build();

        userRepository.save(userEntity);

        var outboxEntity = OutboxEntity.builder()
                .aggregateType("User")
                .aggregateId(userEntity.getId().toString())
                .type("UserCreated")
                .payload(objectMapper.valueToTree(userEntity))
                .build();

        outboxRepository.save(outboxEntity);

        log.info("Transaction completed successfully");
    }

}
