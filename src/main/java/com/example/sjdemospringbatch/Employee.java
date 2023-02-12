package com.example.sjdemospringbatch;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Data
@Table(name = "user_analytics_table")
public class Employee {

    @Id
    private String id;
    private String name;
    private String email;


}
