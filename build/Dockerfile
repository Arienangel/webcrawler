FROM docker.io/library/debian:bookworm-slim

# Install astral-uv
ARG PYTHON_VERSION
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=true
COPY --from=docker.io/astral/uv:bookworm-slim /usr/local/bin/uv /usr/local/bin/uvx /usr/local/bin/
RUN uv python install ${PYTHON_VERSION}

# Install chromium
ARG CHROME_VERSION
RUN apt-get update && apt-get install -y curl xz-utils fonts-noto-cjk libasound2 libatk-bridge2.0-0 libatk1.0-0 libatomic1 libatspi2.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libdrm2 libevent-2.1-7 libexpat1 libflac12 libfontconfig1 libfreetype6 libgbm1 libgcc-s1 libglib2.0-0 libjpeg62-turbo libjsoncpp25 liblcms2-2 libminizip1 libnspr4 libnss3 libopenjp2-7 libopus0 libpango-1.0-0 libpng16-16 libpulse0 libre2-9 libsnappy1v5 libstdc++6 libtiff6 libwebp7 libwebpdemux2 libwebpmux3 libx11-6 libxcb1 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxml2 libxrandr2 libxslt1.1 libxtst6 zlib1g && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN curl -sL -o ungoogled-chromium.tar.xz https://github.com/ungoogled-software/ungoogled-chromium-portablelinux/releases/download/${CHROME_VERSION}/ungoogled-chromium-${CHROME_VERSION}-x86_64_linux.tar.xz && mkdir chromium && tar -xf ungoogled-chromium.tar.xz -C chromium --strip-components=1 && rm -f ungoogled-chromium.tar.xz && ln -s /chromium/chrome /usr/local/bin/chrome

RUN mkdir /app
WORKDIR /app
CMD uv run python
