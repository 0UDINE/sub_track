-- Create the raw database
CREATE DATABASE Raw_Subscription_Data_DB;
GO

-- Use the database
USE Raw_Subscription_Data_DB;
GO

-- Table: raw_User
CREATE TABLE raw_User (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    userId NVARCHAR(100), -- Original ID might be string in source
    email NVARCHAR(255),
    motsDePasse NVARCHAR(255), -- Raw/unhashed or as received from source
    rôle NVARCHAR(100),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100),
    data_quality_flag BIT DEFAULT 0
);

-- Table: raw_Utilisateur_personnel
CREATE TABLE raw_Utilisateur_personnel (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    userId NVARCHAR(100),
    prenom NVARCHAR(100),
    nom NVARCHAR(100),
    telephone NVARCHAR(100), -- Less strict than processed
    adresse NVARCHAR(MAX), -- More flexible length
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100),
    data_quality_notes NVARCHAR(MAX)
);

-- Table: raw_Admin
CREATE TABLE raw_Admin (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    userId NVARCHAR(100),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Table: raw_Utilisateur_fournisseur
CREATE TABLE raw_Utilisateur_fournisseur (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    userId NVARCHAR(100),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Table: raw_Fournisseur
CREATE TABLE raw_Fournisseur (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    idFournisseur NVARCHAR(100), -- Original ID format
    nomFournisseur NVARCHAR(255),
    emailFournisseur NVARCHAR(255),
    numTele NVARCHAR(100),
    addressFournisseur NVARCHAR(MAX),
    secteurFournisseur NVARCHAR(255),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Table: raw_Notifications
CREATE TABLE raw_Notifications (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    idNotification NVARCHAR(100),
    typeNotification NVARCHAR(255), -- Wider than processed
    dateEnvoi NVARCHAR(100), -- Could be string in raw format
    typeEnvoi NVARCHAR(100),
    contenu NVARCHAR(MAX),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Raw junction table for user notifications
CREATE TABLE raw_UtilisateurNotif (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    userId NVARCHAR(100),
    idNotification NVARCHAR(100),
    estLu NVARCHAR(10), -- Could be 'true', 'false', '1', '0' etc.
    estUrgent NVARCHAR(10),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Table: raw_Journal
CREATE TABLE raw_Journal (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    idJournal NVARCHAR(100),
    userId NVARCHAR(100),
    idAbonnement NVARCHAR(100),
    descriptionJournal NVARCHAR(MAX),
    typeActivite NVARCHAR(255),
    dateCreationJournal NVARCHAR(100), -- Could be string in raw
    valeurPrécedente NVARCHAR(MAX),
    nouvelleValeur NVARCHAR(MAX),
    StatusActivite NVARCHAR(255),
    Timestamps NVARCHAR(100),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Table: raw_Abonnement
CREATE TABLE raw_Abonnement (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    idAbonnement NVARCHAR(100),
    idFournisseur NVARCHAR(100),
    titreAbonnement NVARCHAR(255),
    descriptionAbonnement NVARCHAR(MAX),
    prixAbonnement NVARCHAR(100), -- Could have currency symbols
    categorieAbonnement NVARCHAR(255),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Raw junction table for user subscriptions
CREATE TABLE raw_utilisateur_abonement (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    userId NVARCHAR(100),
    idAbonnement NVARCHAR(100),
    typeFacturation NVARCHAR(255),
    statutAbonnement NVARCHAR(255),
    dateRenouvelement NVARCHAR(100),
    dateDebutFacturation NVARCHAR(100),
    prochaineDeFacturation NVARCHAR(100),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Table: raw_Paiement
CREATE TABLE raw_Paiement (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    idPaiement NVARCHAR(100),
    idAbonnement NVARCHAR(100),
    userId NVARCHAR(100),
    methodePaiement NVARCHAR(255),
    datePaiement NVARCHAR(100),
    montantPaiement NVARCHAR(100),
    motifPaiement NVARCHAR(MAX),
    statusPaiement NVARCHAR(100),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);

-- Table: raw_MethodePaiement
CREATE TABLE raw_MethodePaiement (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    idPaiement NVARCHAR(100),
    userId NVARCHAR(100),
    informationCarte NVARCHAR(MAX), -- Could contain full card numbers in raw
    dateExpirementCarte NVARCHAR(100),
    typeMethode NVARCHAR(255),
    date_extraction DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(100)
);