CREATE TABLE Carbonator (
    id int UNIQUE NOT NULL,
    name varchar(30),
	PRIMARY KEY (id)
);

CREATE TABLE Fermenter (
    id int UNIQUE NOT NULL,
    name varchar(30),
	PRIMARY KEY (id)
);

CREATE TABLE Beer (
    id int UNIQUE NOT NULL,
    name varchar(30),
	maduration_temp float,
	fermentation_temp float,
	PRIMARY KEY (id)
);

CREATE TABLE Process (
    id int UNIQUE NOT NULL,
	fecha_inicio TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	fecha_finalizacion TIMESTAMP,
	stage varchar(20),
	state varchar(20),
	fermenter_id INT,
	carbonator_id INT,
	beer_id INT,
	PRIMARY KEY (id),
	FOREIGN KEY (fermenter_id) REFERENCES Fermenter(id),
	FOREIGN KEY (carbonator_id) REFERENCES Carbonator(id),
	FOREIGN KEY (beer_id) REFERENCES Beer(id)
);

CREATE TABLE Temperature (
    id int UNIQUE NOT NULL,
	timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	temperature float,
	process_id INT,
	PRIMARY KEY (id),
	FOREIGN KEY (process_id) REFERENCES Process(id)
);
