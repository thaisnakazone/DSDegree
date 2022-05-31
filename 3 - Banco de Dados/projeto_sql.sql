/*
PROJETO - MÓDULO BANCO DE DADOS
BANCO DE DADOS - LET'S CODE

Neste projeto, vamos desenvolver as tabelas para um banco de dados da Let's Code.
Essas tabelas devem conter dados sobre alunos, turmas, notas e frequências.
Resolva os exercícios abaixo, conforme as instruções.
*/

/*
EXERCÍCIO 1
Crie 3 tabelas para esse banco de dados, sendo elas:

- alunos
Variáveis:
	- aluno_id
	- nome_aluno
	- idade
	- bolsista
	- mensalidade
	
- turmas
Variáveis:
	- turma_id
	- nome_turma
	- professor_id
	- nome_professor
	- carga_horaria
	- dias_semana
	
- notas_freq
Variáveis:
	- turma_id
	- professor_id
	- aluno_id
	- frequencia
	- nota_projeto1
	- nota_projeto2
	- nota_prova
	- nps_prof
*/

-- Excluindo a tabela "alunos", caso ela já exista no banco de dados
DROP TABLE IF EXISTS alunos;

-- Criando a tabela "alunos"
CREATE TABLE alunos(
	aluno_id    SERIAL PRIMARY KEY,
	nome_aluno  VARCHAR(255) NOT NULL,
	idade       INT NOT NULL,
	bolsista    VARCHAR(20),
	mensalidade NUMERIC(8,2)
);

-- Excluindo a tabela "turmas", caso ela já exista no banco de dados
DROP TABLE IF EXISTS turmas;

-- Criando a tabela "turmas"
CREATE TABLE turmas(
	turma_id       SERIAL PRIMARY KEY,
	nome_turma     VARCHAR(255) NOT NULL,
	professor_id   SERIAL NOT NULL,
	nome_professor VARCHAR(255) NOT NULL,
	carga_horaria  INT NOT NULL,
	dias_semana    VARCHAR(255)
);

-- Excluindo a tabela "notas_freq", caso ela já exista no banco de dados
DROP TABLE IF EXISTS notas_freq;

-- Criando a tabela "notas_freq"
CREATE TABLE notas_freq(
	turma_id       SERIAL REFERENCES turmas(turma_id),
	professor_id   SERIAL NOT NULL,
	aluno_id       SERIAL REFERENCES alunos(aluno_id),
	frequencia     INT,
	nota_projeto1  INT,
	nota_projeto2  INT,
	nota_prova     INT,
	nps_prof       INT
);

-- Verificando as tabelas criadas
SELECT * FROM alunos;
SELECT * FROM turmas;
SELECT * FROM notas_freq;

/*
EXERCÍCIO 2
Preencha os dados nas tabelas.
*/

-- Inserindo os dados na tabela "alunos"
INSERT INTO alunos (aluno_id, nome_aluno, idade, bolsista, mensalidade)
VALUES
	(0001, 'João Paulo', 20, 'Não', 600),
	(0002, 'Agatha Cristina', 28, 'Sim', 0),
	(0003, 'Sandro Silva', 35, 'Não', 300),
	(0004, 'Luiza Machado', 24, 'Não', 300),
	(0005, 'Amanda Oliveira', 27, 'Não', 600),
	(0006, 'Rafael Pereira', 44, 'Sim', 0),
	(0007, 'Lucas Pires', 18, 'Não', 600),
	(0008, 'Gustavo Carvalho', 21, 'Não', 300),
	(0009, 'Paulo Vitor', 33, 'Não', 300),
	(0010, 'Nathan Souza', 35, 'Não', 600),
	(0011, 'Julia Menezes', 19, 'Sim', 0),
	(0012, 'Cristiano Hencke', 27, 'Não', 600),
	(0013, 'Monique Santos', 24, 'Não', 300),
	(0014, 'Everton Alex', 24, 'Sim', 0),
	(0015, 'Alexandre Silva', 23, 'Não', 300),
	(0016, 'Pedro Luiz', 20, 'Não', 300),
	(0017, 'Cintia Rodrigues', 18, 'Não', 600),
	(0018, 'Amanda Machado', 29, 'Sim', 0),
	(0019, 'Gustavo Silva', 32, 'Sim', 0),
	(0020, 'Maria Antonia', 34, 'Não', 600),
	(0021, 'Gervasio Oliveira', 65, 'Não', 300),
	(0022, 'Lucas Silva', 30, 'Não', 300),
	(0023, 'Jessica Siqueira', 21, 'Sim', 0),
	(0024, 'Noah Reis', 18, 'Não', 600),
	(0025, 'Ingrid Silva', 18, 'Não', 600),
	(0026, 'Luiz Henrique', 22, 'Sim', 0),
	(0027, 'Antonio Pagano', 25, 'Não', 300),
	(0028, 'Wesley Sousa', 27, 'Não', 300),
	(0029, 'Marisa Santos', 27, 'Não', 300),
	(0030, 'Juliana Melo', 18, 'Não', 600),
	(0031, 'Bruno Kenji', 18, 'Sim', 0),
	(0032, 'Caroline Silva', 31, 'Não', 600),
	(0033, 'Gustavo Marques', 34, 'Não', 600),
	(0034, 'Vinicius Camargo', 25, 'Sim', 0),
	(0035, 'Gisele Marquez', 23, 'Não', 300),
	(0036, 'Jair Vieira', 21, 'Não', 600),
	(0037, 'Paulo Pinheiro', 22, 'Sim', 0),
	(0038, 'Rafael Costa', 30, 'Não', 600),
	(0039, 'Ricardo Novaes', 32, 'Não', 600),
	(0040, 'Sonia Cristina', 22, 'Não', 300),
	(0041, 'Luis antonio', 18, 'Sim', 0),
	(0042, 'Sofia Antonia', 24, 'Não', 600),
	(0043, 'Ana Maria', 26, 'Sim', 0),
	(0044, 'Julio Pieri', 20, 'Não', 600);

-- Verificando a tabela "alunos"
SELECT * FROM alunos;

-- Inserindo os dados na tabela "turmas"
INSERT INTO turmas (turma_id, nome_turma, professor_id, nome_professor, carga_horaria, dias_semana)
VALUES
	(0001, 'Python',         0001, 'Cleber Silva',   80,  'Segunda | Quarta'),
	(0002, 'Data Science',   0002, 'Pedro Henrique', 120, 'Segunda | Quarta | Sexta'),
	(0003, 'Python',         0001, 'Cleber Silva',   80,  'Terça | Quinta'),
	(0004, 'Banco de Dados', 0003, 'Anderson Sousa', 60,  'Segunda | Quarta'),
	(0005, 'Python',         0001, 'Cleber Silva',   80,  'Sábado'),
	(0006, 'Banco de Dados', 0003, 'Anderson Sousa', 60,  'Terça | Quinta'),
	(0007, 'Data Science',   0002, 'Pedro Henrique', 120, 'Sábado');

-- Verificando a tabela "turmas"
SELECT * FROM turmas;

-- Inserindo os dados na tabela "notas_freq"
INSERT INTO notas_freq(turma_id, professor_id, aluno_id, frequencia, nota_projeto1, nota_projeto2, nota_prova, nps_prof)
VALUES
	(0001, 0001, 0001, 90, 8, 10, 7, NULL),
	(0001, 0001, 0002, 95, 10, 9, 10, 90),
	(0001, 0001, 0003, 85, 10, 8.5, 8, 92),
	(0001, 0001, 0004, 100, 9, 9.5, 9.5, 88),
	(0001, 0001, 0005, 100, 9.5, 10, 10, 94),
	(0001, 0001, 0006, 95, 8, 10, 9, 100),
	(0001, 0001, 0007, 190, 10, 7.5, 9, NULL),
	(0002, 0002, 0008, 70, 7, 7.5, 6.5, 75),
	(0002, 0002, 0009, 75, 9, 9, 9, 80),
	(0002, 0002, 0010, 100, 10, 10, 10, 90),
	(0002, 0002, 0011, 80, 8.5, 8, 8.5, 100),
	(0002, 0002, 0012, 95, 9, 10, 7.5, 95),
	(0002, 0002, 0013, 80, 10, 10, 10, 88),
	(0003, 0001, 0014, 100, 8, 10, 10, NULL),
	(0003, 0001, 0015, 80, 9.5, 9, 9, 92),
	(0003, 0001, 0016, 90, 9, 9.5, 10, 90),
	(0003, 0001, 0017, 90, 10, 9, 9, 88),
	(0003, 0001, 0018, 85, 9.5, 8, 10, 95),
	(0003, 0001, 0019, 75, 10, 9.5, 8, 87),
	(0003, 0001, 0020, 90, 9, 8.5, 8, 75),
	(0003, 0001, 0021, 95, 8, 10, 10, 91),
	(0004, 0003, 0001, 100, 10, 10, 10, 100),
	(0004, 0003, 0002, 85, 9.5, 8, 8, NULL),
	(0004, 0003, 0003, 95, 9, 9, 10, 84),
	(0004, 0003, 0004, 85, 8.5, 9, 8.5, 86),
	(0004, 0003, 0005, 80, 9, 7, 7.5, 88),
	(0004, 0003, 0006, 95, 9.5, 10, 9, 90),
	(0004, 0003, 0007, 90, 9, 9, 9.5, 95),
	(0005, 0001, 0022, 100, 10, 9.5, 10, 90),
	(0005, 0001, 0023, 100, 9.5, 10, 10, 100),
	(0005, 0001, 0024, 90, 10, 10, 9, 98),
	(0005, 0001, 0025, 100, 10, 10, 10, NULL),
	(0005, 0001, 0026, 100, 9, 9, 9.5, 100),
	(0005, 0001, 0027, 95, 10, 10, 10, 95),
	(0006, 0003, 0028, 100, 8, 10, 9.5, NULL),
	(0006, 0003, 0029, 80, 8.5, 9.5, 10, 85),
	(0006, 0003, 0030, 85, 9, 8, 8, 82),
	(0006, 0003, 0031, 90, 9, 7.5, 9, 85),
	(0006, 0003, 0032, 75, 8.5, 10, 9, NULL),
	(0006, 0003, 0033, 80, 7.5, 8, 7, 95),
	(0006, 0003, 0034, 35, 9, 3.5, 0, NULL),
	(0006, 0003, 0035, 80, 8, 8, 9.5, 84),
	(0007, 0002, 0036, 70, 7, 7.5, 6.5, 75),
	(0007, 0002, 0037, 75, 9, 9, 9, 80),
	(0007, 0002, 0038, 100, 10, 10, 10, 90),
	(0007, 0002, 0039, 80, 8.5, 8, 8.5, 100),
	(0007, 0002, 0040, 95, 9, 10, 7.5, 95),
	(0007, 0002, 0041, 80, 7, 10, 10, NULL),
	(0007, 0002, 0042, 80, 9, 8.5, 9, 80),
	(0007, 0002, 0043, 80, 8.5, 9, 9, 86),
	(0007, 0002, 0044, 80, 9, 9.5, 10, 94);

-- Verificando a tabela "notas_freq"
SELECT * FROM notas_freq;

/*
EXERCÍCIO 3

Calcule a média do NPS dos professores (arredondando para duas casas decimais), ignorando as notas nulas e ordenando da maior média para a menor.
*/

-- As notas nulas já são ignoradas ao fazer o cálculo
-- Apenas por segurança, coloquei WHERE nps_prof IS NOT NULL

SELECT
	A.nome_professor,
	B.professor_id,
	ROUND(AVG(B.nps_prof), 2) AS media_nps_prof
FROM turmas AS A
LEFT JOIN notas_freq AS B ON A.professor_id = B.professor_id
WHERE B.nps_prof IS NOT NULL
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Solução simples sem trazer o nome dos professores (sem JOIN)
SELECT
	professor_id,
	ROUND(AVG(nps_prof), 2) AS media_nps_prof
FROM notas_freq
WHERE nps_prof IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;

/*
EXERCÍCIO 4
Calcule a média final de cada aluno, em cada turma, sendo a média calculada da seguinte forma: 0.3 * projeto1 + 0.3 * projeto2 + 0.4 * prova.
*/

SELECT
	A.aluno_id,
	A.nome_aluno,
	B.turma_id,
	C.nome_turma,
	(0.3 * B.nota_projeto1 + 0.3 * B.nota_projeto2 + 0.4 * B.nota_prova) AS media_final
FROM alunos AS A
LEFT JOIN notas_freq AS B ON A.aluno_id = B.aluno_id
RIGHT JOIN turmas AS C ON B.turma_id = C.turma_id;

/*
EXERCÍCIO 5
Conte a quantidade de alunos que seriam reprovados por turma, com base no seguinte critério de reprovação: nota final (calculada no exercício anterior) menor do que 7 ou a frequência menor do que 70%.
Ordene da turma com mais reprovados para a com menos.
*/

-- Criando uma View temporária para visualizar a nota final e frequência de cada aluno
CREATE OR REPLACE TEMP VIEW view_media_freq AS
SELECT
	A.aluno_id,
	A.nome_aluno,
	B.turma_id,
	C.nome_turma,
	B.frequencia,
	(0.3 * B.nota_projeto1 + 0.3 * B.nota_projeto2 + 0.4 * B.nota_prova) AS nota_final
FROM alunos AS A
LEFT JOIN notas_freq AS B ON A.aluno_id = B.aluno_id
RIGHT JOIN turmas AS C ON B.turma_id = C.turma_id;

SELECT * FROM view_media_freq;

-- Verificando os dados de quem foi reprovado
SELECT
	*
FROM view_media_freq
WHERE frequencia < 70 OR nota_final < 7;

-- Resposta
SELECT
	turma_id,
	nome_turma,
	COUNT(nome_aluno) AS qtde_reprovados
FROM view_media_freq
WHERE frequencia < 70 OR nota_final < 7
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Mostrando o nome de quem foi reprovado e em qual turma
SELECT
	turma_id,
	nome_turma,
	nome_aluno
FROM view_media_freq
WHERE frequencia < 70 OR nota_final < 7;

-- Testando com outros valores para ver se funciona com mais dados na tabela
SELECT
	*
FROM view_media_freq
WHERE frequencia < 80 OR nota_final < 8;

-- Resposta da questão, se a frequência mínima fosse 80% e a nota mínima fosse 8
SELECT
	turma_id,
	nome_turma,
	COUNT(nome_aluno) AS qtde_reprovados
FROM view_media_freq
WHERE frequencia < 80 OR nota_final < 8
GROUP BY 1, 2
ORDER BY 3 DESC;

-- Nome e turma dos reprovados, se a frequência mínima fosse 80% e a nota mínima fosse 8
SELECT
	turma_id,
	nome_turma,
	nome_aluno
FROM view_media_freq
WHERE frequencia < 80 OR nota_final < 8;

/*
EXERCÍCIO 6
Conte a quantidade de bolsistas matriculados por turma.
*/

-- Criando uma View temporária para visualizar os bolsistas em cada turma
CREATE OR REPLACE TEMP VIEW view_bolsistas AS
SELECT
	A.turma_id,
	A.nome_turma,
	C.aluno_id,
	C.bolsista
FROM turmas AS A
LEFT JOIN notas_freq AS B ON A.turma_id = B.turma_id
LEFT JOIN alunos AS C ON B.aluno_id = C.aluno_id
GROUP BY 1, 3
HAVING C.bolsista = 'Sim'
ORDER BY 1;

SELECT * FROM view_bolsistas;

-- Resposta
SELECT
	turma_id,
	nome_turma,
	COUNT(bolsista) AS qtde_bolsistas
FROM view_bolsistas
GROUP BY 1, 2;

/*
EXERCÍCIO 7
Calcule a média de idade por turma (arredondando para 1 casa decimal) e a maior idade por turma.
*/

-- Criando uma View temporária para visualizar a idade dos alunos em cada turma
CREATE OR REPLACE TEMP VIEW view_idade AS
SELECT
	A.turma_id,
	A.nome_turma,
	C.aluno_id,
	C.idade
FROM turmas AS A
LEFT JOIN notas_freq AS B ON A.turma_id = B.turma_id
LEFT JOIN alunos AS C ON B.aluno_id = C.aluno_id
GROUP BY 1, 3
ORDER BY 1;

SELECT * FROM view_idade;

-- Resposta
SELECT
	turma_id,
	nome_turma,
	ROUND(AVG(idade), 1) AS media_idade,
	MAX(idade) AS max_idade
FROM view_idade
GROUP BY 1, 2
ORDER BY 1;
	
/*
EXERCÍCIO 8
Calcule o total faturado por turma, que seria o valor da mensalidade paga pelos alunos.
Faça esse cálculo agrupado por turma_id e nome_turma.
*/

-- Criando uma View temporária para visualizar as mensalidades pagas por aluno em cada turma
CREATE OR REPLACE TEMP VIEW view_mensalidade AS
SELECT
	A.turma_id,
	A.nome_turma,
	C.aluno_id,
	C.mensalidade
FROM turmas AS A
LEFT JOIN notas_freq AS B ON A.turma_id = B.turma_id
LEFT JOIN alunos AS C ON B.aluno_id = C.aluno_id
GROUP BY 1, 3
ORDER BY 1;

SELECT * FROM view_mensalidade;
	
-- Resposta
SELECT
	turma_id,
	nome_turma,
	SUM(mensalidade) AS total_faturado
FROM view_mensalidade
GROUP BY 1, 2
ORDER BY 1;

/*
EXERCÍCIO 9
Calcule quanto cada um dos professores recebeu por turma.
O salário do professor é: 5% do total das mensalidades * carga horário do curso.

Dica: Você irá utilizar algo como: AVG(carga_horaria) * SUM(0.05 * mensalidade).
*/

-- Criando uma View temporária para visualizar: turmas, professores, cargas horárias, alunos e mensalidades
CREATE OR REPLACE TEMP VIEW view_prof_mensal AS
SELECT
	A.turma_id,
	A.nome_turma,
	A.nome_professor,
	A.carga_horaria,
	C.aluno_id,
	C.mensalidade
FROM turmas AS A
LEFT JOIN notas_freq AS B ON A.turma_id = B.turma_id
LEFT JOIN alunos AS C ON B.aluno_id = C.aluno_id
GROUP BY 1, 5
ORDER BY 1;

SELECT * FROM view_prof_mensal;

-- Resposta
SELECT
	turma_id,
	nome_turma,
	nome_professor,
	ROUND((AVG(carga_horaria) * SUM(0.05 * mensalidade)), 2) AS salario_prof
FROM view_prof_mensal
GROUP BY 1, 2, 3
ORDER BY 1;

/*
EXERCÍCIO 10
Utilizando a variável dias_semana, que representa os dias em que aconteciam as aulas, calcule a quantidade de alunos por dias_semana, ordenando da maior para a menor quantidade.
*/

-- Criando uma View temporária para visualizar os alunos em "dias_semana"
CREATE OR REPLACE TEMP VIEW view_alunos_semana AS
SELECT
	A.turma_id,
	A.nome_turma,
	C.aluno_id,
	A.dias_semana
FROM turmas AS A
LEFT JOIN notas_freq AS B ON A.turma_id = B.turma_id
LEFT JOIN alunos AS C ON B.aluno_id = C.aluno_id
GROUP BY 1, 3
ORDER BY 1;

SELECT * FROM view_alunos_semana;

-- Resposta
SELECT
	dias_semana,
	COUNT(aluno_id) AS qtde_alunos
FROM view_alunos_semana
GROUP BY 1
ORDER BY 2 DESC;
