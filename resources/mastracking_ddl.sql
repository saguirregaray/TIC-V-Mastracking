CREATE TABLE Carbonators (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
    name varchar(30),
    deleted boolean NOT NULL DEFAULT false,
	PRIMARY KEY (id)
);

CREATE TABLE Fermenters (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
    name varchar(30),
    deleted boolean NOT NULL DEFAULT false,
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
	state varchar(20),
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
	process_id INT,
	deleted boolean NOT NULL DEFAULT false,
	PRIMARY KEY (id),
	FOREIGN KEY (process_id) REFERENCES Processes(id)
);

CREATE TABLE Alerts (
    id int UNIQUE NOT NULL AUTO_INCREMENT,
    description varchar(500),
    deleted boolean NOT NULL DEFAULT false,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (id)
);
