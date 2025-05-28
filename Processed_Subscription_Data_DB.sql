-- Create the processed database
CREATE DATABASE Processed_Subscription_Data_DB;
GO

-- Use the database
USE Processed_Subscription_Data_DB;
GO

-- Table: processed_User
CREATE TABLE processed_User (
    userId VARCHAR(15) PRIMARY KEY,
    email NVARCHAR(255) NOT NULL,
    motsDePasse NVARCHAR(255) NOT NULL, 
    role NVARCHAR(50) NOT NULL
);
-- forcer la forme du email 
ALTER TABLE processed_User	
ADD CONSTRAINT CHK_valid_email CHECK (email LIKE '%_@__%.__%');

-- Table: processed_Utilisateur_personnel (inherits processed_User)
CREATE TABLE processed_Utilisateur_personnel (
    userId VARCHAR(15) PRIMARY KEY,
    prenom NVARCHAR(100),
    nom NVARCHAR(100),
    telephone NVARCHAR(50),
    adresse NVARCHAR(255),

    FOREIGN KEY (userId) REFERENCES processed_User(userId) 
	ON DELETE CASCADE  
    ON UPDATE CASCADE
);
-- checking the number of digits of phone number is excatly 10 digits 
ALTER TABLE processed_Utilisateur_personnel
ADD CONSTRAINT CK_processed_Utilisateur_personnel_telephone_check_nbr_digits_telephone CHECK (LEN(telephone) = 10 OR telephone IS NULL); -- Allow NULL values

-- Table: processed_Admin (hérite processed_User)
CREATE TABLE processed_Admin (
    userId VARCHAR(15) PRIMARY KEY,

    FOREIGN KEY (userId) REFERENCES processed_User(userId)
	ON DELETE CASCADE  
    ON UPDATE CASCADE
);

-- Table: processed_Utilisateur_fournisseur (hérite processed_User)
CREATE TABLE processed_Utilisateur_fournisseur (
    userId VARCHAR(15) PRIMARY KEY,

    FOREIGN KEY (userId) REFERENCES processed_User(userId)
	ON DELETE CASCADE
    ON UPDATE CASCADE
);

-- Table: processed_Fournisseur
CREATE TABLE processed_Fournisseur (
    idFournisseur VARCHAR(15) PRIMARY KEY,
    nomFournisseur NVARCHAR(255),
    emailFournisseur NVARCHAR(255),
    numTele NVARCHAR(50),
    addressFournisseur NVARCHAR(255),
    secteurFournisseur NVARCHAR(100)
);
-- plage de valeur possible pour secteurFournisseur
ALTER TABLE processed_Fournisseur
ADD CONSTRAINT CHK_secteurFournisseur 
CHECK (secteurFournisseur IN ('Industrie', 'Agriculture', 'Technologie', 'Santé', 'Construction', 'Transport', 'Commerce'));

-- Table: processed_Abonnement (moved before Journal to fix dependency)
CREATE TABLE processed_Abonnement (
    idAbonnement VARCHAR(15) PRIMARY KEY,
	idFournisseur VARCHAR(15),
    titreAbonnement NVARCHAR(255),
    descriptionAbonnement NVARCHAR(MAX),
    prixAbonnement  DECIMAL(10,2),
    categorieAbonnement NVARCHAR(100),

	FOREIGN KEY (idFournisseur) REFERENCES processed_Fournisseur(idFournisseur) ON DELETE CASCADE ON UPDATE CASCADE
);
--plage des valeur de categorieAbonnement
ALTER TABLE processed_Abonnement
ADD CONSTRAINT CHK_categorieAbonnement CHECK(categorieAbonnement IN ('STREAMING','MUSIC','CLOUD','FORFAIT APPEL CONX'));
ALTER TABLE processed_Journal NOCHECK CONSTRAINT FK__processed__idAbo__52593CB8

-- Table: processed_Notifications
CREATE TABLE processed_Notifications (
    idNotification VARCHAR(15) PRIMARY KEY,
    typeNotification NVARCHAR(100),
    dateEnvoi DATETIME,
    typeEnvoi NVARCHAR(100),
    contenu NVARCHAR(MAX)
);

-- plage de valeur possible pour typeNotification 
ALTER TABLE processed_Notifications
ADD CONSTRAINT CHK_typeNotif CHECK (typeNotification IN ('PAYMENT_REMINDER','SUBSCRIPTION_EXPIRY','PAYMENT_CONFIRMATION','SUBSCRIPTION_RENEWAL','PRICE_CHANGE','SYSTEM_ALERT'));

-- table de jonction (entité associative) entre utilisateur et notification 
-- puisque c est une relation plusieurs à plusieurs
CREATE TABLE UtilisateurNotif(
	 userId VARCHAR(15) NOT NULL,
	 idNotification VARCHAR(15) NOT NULL,
	 estLu BIT,
	 estUrgent BIT,

	 PRIMARY KEY (userId,idNotification),
	 FOREIGN KEY (userId) REFERENCES processed_Utilisateur_personnel(userId) ON DELETE CASCADE ON UPDATE CASCADE,
     FOREIGN KEY (idNotification) REFERENCES processed_Notifications(idNotification) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Table: processed_Journal
CREATE TABLE processed_Journal (
    idJournal VARCHAR(15) PRIMARY KEY,
	userId VARCHAR(15),
	idAbonnement VARCHAR(15),
    descriptionJournal NVARCHAR(MAX),
    typeActivite NVARCHAR(100),
    dateCreationJournal DATETIME,
    valeurPrécedente NVARCHAR(MAX),
    nouvelleValeur NVARCHAR(MAX),
    StatusActivite NVARCHAR(100),
    Timestamps DATETIME,

	FOREIGN KEY (userId) REFERENCES processed_User(userId) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (idAbonnement) REFERENCES processed_Abonnement(idAbonnement) ON DELETE CASCADE ON UPDATE CASCADE
);
-- pale de valeur pour StatusActivite
ALTER TABLE processed_Journal
ADD CONSTRAINT CH_statusActivite CHECK (StatusActivite IN ('CREATION_ABONEMMENT','ANNULATION_ABONEMMENT','ECHOUE_PAIEMENT','PAIMENT_SUCCCES','PRIX_ABONEMENT_CHANGE'));

-- table de jonction entre table utilisateur personnel et table abonements
CREATE TABLE utilisateur_abonement(
	userId VARCHAR(15),
	idAbonnement VARCHAR(15),
	typeFacturation NVARCHAR(100), 
	statutAbonnement NVARCHAR(100),
	dateRenouvelement DATE,
	dateDebutFacturation DATE,
    prochaineDeFacturation DATE,

	PRIMARY KEY (idAbonnement,userId),
	FOREIGN KEY (idAbonnement) REFERENCES processed_Abonnement(idAbonnement) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (userId) REFERENCES processed_Utilisateur_personnel(userId) ON DELETE CASCADE ON UPDATE CASCADE
);

--plage des valeur de statutAbonnement
ALTER TABLE utilisateur_abonement
ADD CONSTRAINT CHK_statutAbonnement CHECK(statutAbonnement IN ('ACTIF','EXPIRE','ANNULE'));

--plage des valeur de typeFacturation
ALTER TABLE utilisateur_abonement
ADD CONSTRAINT CHK_typeFacturation CHECK(typeFacturation IN ('MENSUAL','ANNUEL','QUOTIDIEN','HEBDOMADAIRE'));

-- assure que les futures dates pour abonements soit plus grands
ALTER TABLE utilisateur_abonement
ADD CONSTRAINT CHK_future_billing_dates CHECK (
    dateDebutFacturation <= prochaineDeFacturation
    AND dateRenouvelement >= dateDebutFacturation
);

-- Table: processed_Paiement
CREATE TABLE processed_Paiement (
    idPaiement VARCHAR(15) PRIMARY KEY,
	idAbonnement VARCHAR(15),
	userId VARCHAR(15),
    methodePaiement NVARCHAR(100),
    datePaiement DATE,
    montantPaiement DECIMAL(10,2),
    motifPaiement NVARCHAR(MAX),
    statusPaiement NVARCHAR(100), -- Added missing column

	FOREIGN KEY (userId) REFERENCES processed_Utilisateur_personnel(userId) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (idAbonnement) REFERENCES processed_Abonnement(idAbonnement) ON DELETE CASCADE ON UPDATE CASCADE
);
--plage de valeur statusPaiement (fixed column name)
ALTER TABLE processed_Paiement
ADD CONSTRAINT CHK_statusPaiement CHECK(statusPaiement IN ('REUSSI','ECHOUE'));

-- Table: processed_MethodePaiement
CREATE TABLE processed_MethodePaiement (
    idPaiement VARCHAR(15),
    userId VARCHAR(15),
    informationCarte NVARCHAR(255),
    dateExpirementCarte DATE,
    typeMethode NVARCHAR(100),

	PRIMARY KEY (idPaiement,userId),
	FOREIGN KEY (idPaiement) REFERENCES processed_Paiement(idPaiement) ,
    FOREIGN KEY (userId) REFERENCES processed_Utilisateur_personnel(userId) 
);
--plage de valeur typeMethode (fixed constraint name)
ALTER TABLE processed_MethodePaiement
ADD CONSTRAINT CHK_typeMethode CHECK(typeMethode IN ('CARTE_CREDIT','VIREMENT'));

ALTER TABLE processed_MethodePaiement
ALTER COLUMN dateExpirementCarte NVARCHAR(100);
