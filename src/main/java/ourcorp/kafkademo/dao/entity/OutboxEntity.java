package ourcorp.kafkademo.dao.entity;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import static jakarta.persistence.GenerationType.IDENTITY;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import static org.hibernate.type.SqlTypes.JSON;

@Entity
@Table(name = "outbox")
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class OutboxEntity {

    @Id
    @GeneratedValue(strategy = IDENTITY)
    private Long id;

    @Column(name = "aggregatetype", nullable = false)
    private String aggregateType;

    @Column(name = "aggregateid", nullable = false)
    private String aggregateId;

    @Column(name = "type", nullable = false)
    private String type;

    @JdbcTypeCode(JSON)
    @Column(name = "payload", columnDefinition = "jsonb")
    private JsonNode payload;

}