package com.example.sjdemospringbatch;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Data
@Table(name = "person")
public class Person {

    @Id
    private String id;
    private String name;
    private String email;


}
