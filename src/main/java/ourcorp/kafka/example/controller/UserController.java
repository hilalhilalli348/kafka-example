package ourcorp.kafka.example.controller;

import lombok.RequiredArgsConstructor;
import static org.springframework.http.HttpStatus.CREATED;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ourcorp.kafka.example.model.request.UserCreatedRequest;
import ourcorp.kafka.example.producer.UserProducer;

@RequiredArgsConstructor
@RestController
@RequestMapping("api/v1/users")
public class UserController {

    private final UserProducer userProducer;

    @ResponseStatus(value = CREATED)
    @PostMapping
    public void create(@RequestBody UserCreatedRequest request) {
        userProducer.createUser(request);
    }

}
