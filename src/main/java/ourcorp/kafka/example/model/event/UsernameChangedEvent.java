package ourcorp.kafka.example.model.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class UsernameChangedEvent {

    private String oldUsername;
    private String newUsername;

}
