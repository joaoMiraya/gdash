# Estágio de build
FROM node:22.12-alpine AS build

WORKDIR /app

# Copia os arquivos de dependências
COPY package*.json ./

# Instala as dependências
RUN npm ci

# Copia o código fonte
COPY . .

# Faz o build da aplicação
RUN npm run build

# Estágio de produção
FROM nginx:alpine

# Copia os arquivos buildados para o nginx
COPY --from=build /app/dist /usr/share/nginx/html

# Copia configuração customizada do nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
