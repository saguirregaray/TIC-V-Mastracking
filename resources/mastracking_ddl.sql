CREATE TABLE Carbonators (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
    name varchar(30),
    deleted boolean NOT NULL DEFAULT false,
    physical_id int NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE Fermenters (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
    name varchar(30),
    deleted boolean NOT NULL DEFAULT false,
    physical_id int NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE Beers (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
    name varchar(30),
	maduration_temp float,
	fermentation_temp float,
	deleted boolean NOT NULL DEFAULT false,
	PRIMARY KEY (id)
);

CREATE TABLE Processes (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
	fecha_inicio TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	fecha_finalizacion TIMESTAMP,
	stage varchar(20),
	state bool,
	fermenter_id INT,
	carbonator_id INT,
	beer_id INT,
	deleted boolean NOT NULL DEFAULT false,
	PRIMARY KEY (id),
	FOREIGN KEY (fermenter_id) REFERENCES Fermenters(id),
	FOREIGN KEY (carbonator_id) REFERENCES Carbonators(id),
	FOREIGN KEY (beer_id) REFERENCES Beers(id)
);

CREATE TABLE Temperatures (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
	timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	temperature float,
	target_temperature float,
	process_id INT,
	deleted boolean NOT NULL DEFAULT false,
	PRIMARY KEY (id),
	FOREIGN KEY (process_id) REFERENCES Processes(id)
);

CREATE TABLE Alerts (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
    process_id int,
    alert_timestamp timestamp,
    stage varchar(20),
    description varchar(500),
    deleted boolean NOT NULL DEFAULT false,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (id)
);
