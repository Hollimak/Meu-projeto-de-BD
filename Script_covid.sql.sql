
CREATE OR REPLACE FUNCTION funcao_dados()
RETURNS TABLE 
(
    id INT,
    data_inicio_sintomas DATE,
    sintomas VARCHAR,
    comorbidades VARCHAR(100),
    gestante VARCHAR(100),
    sexo VARCHAR,
    idade VARCHAR,    
    raca VARCHAR,
    bairro VARCHAR,
    codigo_ibge INT,
    nome_municipio VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    estado VARCHAR,
    data_coleta VARCHAR,
    data_resultado TIMESTAMP,
    criterio_confirmacao VARCHAR,
    tipo_teste VARCHAR,
    fez_teste_rapido VARCHAR,
    fez_pcr VARCHAR,
    municipio_notificacao VARCHAR,
    latitude_notificacao VARCHAR,
    longitude_notificacao VARCHAR,
    classificacao VARCHAR,
    origem_esus VARCHAR,
    origem_sivep VARCHAR,
    origem_lacen VARCHAR,
    origem_laboratorio_privado VARCHAR,
    nom_laboratorio VARCHAR,
    data_internacao VARCHAR,
    data_entrada_uti VARCHAR,
    regiao_evolucao_caso VARCHAR,
    data_evolucao_caso VARCHAR,
    data_saida_uti VARCHAR,
    recuperado VARCHAR,
    obito VARCHAR,
    data_obito VARCHAR
)

 AS $$
 BEGIN

DROP TABLE IF EXISTS localizacao CASCADE;
DROP TABLE IF EXISTS resultados_testes CASCADE;
DROP TABLE IF EXISTS notificacoes CASCADE;
DROP TABLE IF EXISTS internacoes CASCADE;
DROP TABLE IF EXISTS recuperados CASCADE;
DROP TABLE IF EXISTS obito CASCADE;
DROP TABLE IF EXISTS pacientes;
DROP TABLE IF EXISTS dados_paciente;

CREATE TABLE dados_paciente (
    id SERIAL PRIMARY KEY,
    data_publicacao TIMESTAMP,
    recuperados VARCHAR(3),
    data_inicio_sintomas DATE,
    data_coleta VARCHAR(255),
    sintomas VARCHAR(255),
    comorbidades VARCHAR(255),
    gestante VARCHAR(255),
    internacao VARCHAR(255),
    internacao_uti VARCHAR(255),
    sexo VARCHAR(10),
    municipio VARCHAR(255),
    obito VARCHAR(3),
    data_obito VARCHAR(100),
    idade VARCHAR(5),
    regional VARCHAR(255),
    raca VARCHAR(50),
    data_resultado TIMESTAMP,
    codigo_ibge_municipio INT,
    latitude VARCHAR(255),
    longitude VARCHAR(255),
    estado VARCHAR(50),
    criterio_confirmacao VARCHAR(255),
    tipo_teste VARCHAR(255),
    municipio_notificacao VARCHAR(255),
    codigo_ibge_municipio_notificacao VARCHAR(100),
    latitude_notificacao VARCHAR(100),
    longitude_notificacao VARCHAR(100),
    classificacao VARCHAR(50),
    origem_esus VARCHAR(3),
    origem_sivep VARCHAR(3),
    origem_lacen VARCHAR(3),
    origem_laboratorio_privado VARCHAR(3),
    nom_laboratorio VARCHAR(255),
    fez_teste_rapido VARCHAR(3),
    fez_pcr VARCHAR(3),
    data_internacao VARCHAR(100),
    data_entrada_uti VARCHAR(100),
    regional_saude VARCHAR(255),
    data_evolucao_caso VARCHAR(100),
    data_saida_uti VARCHAR(100),
    bairro VARCHAR(255)
);

COPY dados_paciente (data_publicacao, recuperados, data_inicio_sintomas, data_coleta, sintomas, comorbidades, gestante, internacao, internacao_uti, sexo, municipio, obito, data_obito, idade, 
regional, raca, data_resultado, codigo_ibge_municipio, latitude, longitude, estado, criterio_confirmacao, tipo_teste, municipio_notificacao, codigo_ibge_municipio_notificacao, latitude_notificacao,
longitude_notificacao, classificacao, origem_esus, origem_sivep, origem_lacen, origem_laboratorio_privado, nom_laboratorio, fez_teste_rapido, fez_pcr, data_internacao, data_entrada_uti,
regional_saude, data_evolucao_caso, data_saida_uti, bairro) FROM '/dados/dadoscovid.csv' DELIMITER ';' CSV HEADER;

--tabelas
CREATE TABLE pacientes 
(
    id  SERIAL PRIMARY KEY,
    data_inicio_sintomas DATE,
    sintomas VARCHAR(255),
    comorbidades VARCHAR(255),
    gestante VARCHAR(255),
    sexo VARCHAR(10),
    idade VARCHAR(5),
    raca VARCHAR(50),
    bairro VARCHAR(255)
);

CREATE TABLE localizacao
(
id SERIAL PRIMARY KEY,
paciente_id INT REFERENCES pacientes(id) NOT NULL,
codigo_ibge INT,
nome_municipio VARCHAR (255),
latitude VARCHAR(255),
longitude VARCHAR(255),
estado VARCHAR(50)
);

CREATE TABLE resultados_testes 
(
    id SERIAL PRIMARY KEY,
    paciente_id INT REFERENCES pacientes(id) NOT NULL,
    data_coleta VARCHAR(255),
    data_resultado TIMESTAMP,
    criterio_confirmacao VARCHAR(255),
    tipo_teste VARCHAR(50),
    fez_teste_rapido VARCHAR(3),
    fez_pcr VARCHAR(3)
);

CREATE TABLE notificacoes 
(
    id SERIAL PRIMARY KEY,
    paciente_id INT REFERENCES pacientes(id) NOT NULL,
    municipio_notificacao VARCHAR(255), 
    latitude_notificacao VARCHAR(255),
    longitude_notificacao VARCHAR(255),
    classificacao VARCHAR(50),
    origem_esus VARCHAR(3),
    origem_sivep VARCHAR(3),
    origem_lacen VARCHAR(3),
    origem_laboratorio_privado VARCHAR(3),
    nom_laboratorio VARCHAR(255)
);

CREATE TABLE internacoes 
(
    id SERIAL PRIMARY KEY,
    paciente_id INT REFERENCES pacientes(id) NOT NULL, 
    data_internacao VARCHAR(100),
    data_entrada_uti VARCHAR(100),
    regiao_evolucao_caso VARCHAR(255),
    data_evolucao_caso VARCHAR,
    data_saida_uti VARCHAR(255)
);

CREATE TABLE recuperados
(
    id SERIAL PRIMARY KEY,
    paciente_id INT REFERENCES pacientes(id) NOT NULL,
    recuperado VARCHAR(3)
);


CREATE TABLE obito
(
    id SERIAL PRIMARY KEY,
    paciente_id INT REFERENCES pacientes(id) NOT NULL,
    obito VARCHAR(3),
    data_obito VARCHAR(100)
);

--insecção de dados
INSERT INTO pacientes (data_inicio_sintomas, sintomas, comorbidades, gestante, sexo, idade, raca, bairro)
SELECT DISTINCT dp.data_inicio_sintomas, dp.sintomas, dp.comorbidades, dp.gestante, dp.sexo, dp.idade, dp.raca, dp.bairro FROM dados_paciente dp;

INSERT INTO localizacao (paciente_id, codigo_ibge, nome_municipio, latitude, longitude, estado)
SELECT DISTINCT p.id, dp.codigo_ibge_municipio, dp.municipio, dp.latitude, dp.longitude, dp.estado FROM dados_paciente dp
JOIN pacientes p ON p.id = dp.id;

INSERT INTO resultados_testes (paciente_id, data_coleta, data_resultado, criterio_confirmacao, tipo_teste,
fez_teste_rapido, fez_pcr)
SELECT DISTINCT p.id, dp.data_coleta, dp.data_resultado, dp.criterio_confirmacao, dp.tipo_teste, 
dp.fez_teste_rapido, dp.fez_pcr FROM dados_paciente dp 
JOIN pacientes p ON p.id = dp.id;

INSERT INTO notificacoes (paciente_id, municipio_notificacao, latitude_notificacao, longitude_notificacao,
classificacao, origem_esus, origem_sivep, origem_lacen, origem_laboratorio_privado, nom_laboratorio)
SELECT DISTINCT p.id, dp.municipio_notificacao, dp.latitude_notificacao, dp.longitude_notificacao, dp.classificacao,
dp.origem_esus, dp.origem_sivep, dp.origem_lacen, dp.origem_laboratorio_privado, dp.nom_laboratorio FROM dados_paciente dp 
JOIN pacientes p ON p.id = dp.id;

INSERT INTO internacoes (paciente_id, data_internacao, data_entrada_uti, regiao_evolucao_caso,
data_evolucao_caso, data_saida_uti)
SELECT DISTINCT p.id, dp.data_internacao, dp.data_entrada_uti, dp.regional_saude, dp.data_evolucao_caso, dp.data_saida_uti FROM dados_paciente dp 
JOIN pacientes p ON p.id = dp.id;

INSERT INTO recuperados (paciente_id, recuperado)
SELECT p.id, dp.recuperados FROM dados_paciente dp JOIN pacientes p ON p.id = dp.id;

INSERT INTO obito (paciente_id, obito, data_obito)
SELECT p.id, dp.obito, dp.data_obito FROM dados_paciente dp JOIN pacientes p ON p.id = dp.id;



-- CRIPTOGRAFANDO DADOS
-- ADICIONANDO extensao pgcrypto
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ALTERANDO O NOME DAS COLUNAS QUE VAI SER CRIPTOGRAFADA
ALTER TABLE pacientes ADD COLUMN sintomas_criptografado BYTEA;
ALTER TABLE pacientes ADD COLUMN sexo_criptografado BYTEA;
ALTER TABLE pacientes ADD COLUMN idade_criptografado BYTEA;
ALTER TABLE pacientes ADD COLUMN raca_criptografado BYTEA;
ALTER TABLE pacientes ADD COLUMN bairro_criptografado BYTEA;

-- CRIPTOGRAFANDO AS COLUNAS
UPDATE pacientes SET sintomas_criptografado = pgp_sym_encrypt(pacientes.sintomas, '1567');
UPDATE pacientes SET sexo_criptografado = pgp_sym_encrypt(pacientes.sexo, '1567');
UPDATE pacientes SET idade_criptografado = pgp_sym_encrypt(pacientes.idade, '1567');
UPDATE pacientes SET raca_criptografado = pgp_sym_encrypt(pacientes.raca, '1567');
UPDATE pacientes SET bairro_criptografado = pgp_sym_encrypt(pacientes.bairro, '1567');

-- Removendo as colunas que foram substituidas 
ALTER TABLE pacientes DROP COLUMN sintomas;
ALTER TABLE pacientes DROP COLUMN sexo;
ALTER TABLE pacientes DROP COLUMN idade;
ALTER TABLE pacientes DROP COLUMN raca;
ALTER TABLE pacientes DROP COLUMN bairro;


CREATE OR REPLACE VIEW dados_view AS
SELECT 
    p.id AS paciente_id,
    p.data_inicio_sintomas,
    pgp_sym_decrypt(p.sintomas_criptografado, '1567')::VARCHAR AS sintomas_descriptografado,
    p.comorbidades,
    p.gestante,
    pgp_sym_decrypt(p.sexo_criptografado, '1567')::VARCHAR AS sexo_descriptografado,
    pgp_sym_decrypt(p.idade_criptografado, '1567')::VARCHAR AS idade_descriptografado,
    pgp_sym_decrypt(p.raca_criptografado, '1567')::VARCHAR AS raca_descriptografado,
    pgp_sym_decrypt(p.bairro_criptografado, '1567')::VARCHAR AS bairro_descriptografado,
    l.codigo_ibge,
    l.nome_municipio,
    l.latitude AS latitude_localizacao,
    l.longitude AS longitude_localizacao,
    l.estado AS estado_localizacao,
    rt.data_coleta,
    rt.data_resultado,
    rt.criterio_confirmacao,
    rt.tipo_teste,
    rt.fez_teste_rapido,
    rt.fez_pcr,
    n.municipio_notificacao,
    n.latitude_notificacao,
    n.longitude_notificacao,
    n.classificacao,
    n.origem_esus,
    n.origem_sivep,
    n.origem_lacen,
    n.origem_laboratorio_privado,
    n.nom_laboratorio,
    i.data_internacao,
    i.data_entrada_uti,
    i.regiao_evolucao_caso,
    i.data_evolucao_caso,
    i.data_saida_uti,
    rec.recuperado,
    ob.obito,
    ob.data_obito

FROM pacientes p
JOIN localizacao l ON p.id = l.paciente_id
JOIN resultados_testes rt ON p.id = rt.paciente_id
JOIN notificacoes n ON p.id = n.paciente_id
JOIN internacoes i ON p.id = i.paciente_id
JOIN recuperados rec ON p.id = rec.paciente_id
JOIN obito ob ON p.id = ob.paciente_id;

RETURN QUERY SELECT * FROM dados_view;


END $$ LANGUAGE plpgsql;
