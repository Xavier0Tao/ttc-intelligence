# Build stage
FROM node:20-alpine AS build

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .
RUN npm run build

# Serve stage: nginx serves the static bundle and proxies /api + /ws to the
# gateway so the dashboard is single-origin.
FROM nginx:alpine

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
