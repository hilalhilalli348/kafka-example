package ourcorp.kafka.example.model.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class UsernameChangedRequest {

    private String oldUsername;
    private String newUsername;

}
