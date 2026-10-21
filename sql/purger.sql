-- Purge RGPD (limitation de la conservation), lancée à chaque exécution du pipeline.
-- Événements bruts et quarantaine : 30 jours. Fiches : 6 mois. Mesures et alertes : 12 mois.
-- Le registre des fichiers ne contient que des noms de fichiers (aucune donnée personnelle).

DELETE FROM pipeline.evenements_bruts WHERE recu_le   < now() - interval '30 days';  -- et sa quarantaine
DELETE FROM pipeline.fiches           WHERE charge_le < now() - interval '6 months';
DELETE FROM pipeline.mesures          WHERE debut     < now() - interval '12 months';
DELETE FROM pipeline.alertes          WHERE cree_le   < now() - interval '12 months';
