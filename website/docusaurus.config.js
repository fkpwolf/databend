// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const TwitterSvg =
    '<svg style="fill: #1DA1F2; vertical-align: middle;" width="16" height="16" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M459.37 151.716c.325 4.548.325 9.097.325 13.645 0 138.72-105.583 298.558-298.558 298.558-59.452 0-114.68-17.219-161.137-47.106 8.447.974 16.568 1.299 25.34 1.299 49.055 0 94.213-16.568 130.274-44.832-46.132-.975-84.792-31.188-98.112-72.772 6.498.974 12.995 1.624 19.818 1.624 9.421 0 18.843-1.3 27.614-3.573-48.081-9.747-84.143-51.98-84.143-102.985v-1.299c13.969 7.797 30.214 12.67 47.431 13.319-28.264-18.843-46.781-51.005-46.781-87.391 0-19.492 5.197-37.36 14.294-52.954 51.655 63.675 129.3 105.258 216.365 109.807-1.624-7.797-2.599-15.918-2.599-24.04 0-57.828 46.782-104.934 104.934-104.934 30.213 0 57.502 12.67 76.67 33.137 23.715-4.548 46.456-13.32 66.599-25.34-7.798 24.366-24.366 44.833-46.132 57.827 21.117-2.273 41.584-8.122 60.426-16.243-14.292 20.791-32.161 39.308-52.628 54.253z"></path></svg>';

const lightCodeTheme = require('prism-react-renderer/themes/oceanicNext');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: 'Databend',
    staticDirectories: ['static', '../docs/public'],
    tagline: 'Databend is a modern cloud data warehouse that empowers your object storage for real-time analytics.',
    url: 'https://databend.rs',
    baseUrl: '/',
    onBrokenLinks: 'warn',
    onBrokenMarkdownLinks: 'throw',
    favicon: 'img/logo/logo-no-text.svg',
    organizationName: 'datafuselabs',
    projectName: 'databend',

    i18n: {
        defaultLocale: 'en-US',
        locales: ['en-US', 'zh-CN'],
        localeConfigs: {
            'en-US': {
                label: 'English',
            },
            'zh-CN': {
                label: '简体中文',
            },
        },
    },

    presets: [
        [
            '@docusaurus/preset-classic',
            /** @type {import('@docusaurus/preset-classic').Options} */
            ({
                docs: {
                    path: '../docs/doc',
                    routeBasePath: 'doc',
                    sidebarPath: require.resolve('../docs/doc/sidebars.js'),
                    editUrl: ({locale, docPath}) => {
                        if (locale !== config.i18n.defaultLocale) {
                          return `https://databend.crowdin.com/databend/${locale}`;
                        }
                        return `https://github.com/datafuselabs/databend/edit/main/docs/doc/${docPath}`;
                      },
                },
                blog: {
                    showReadingTime: true,
                    editUrl: ({locale, blogPath}) => {
                        if (locale !== config.i18n.defaultLocale) {
                          return `https://databend.crowdin.com/databend/${locale}`;
                        }
                        return `https://github.com/datafuselabs/databend/edit/main/website/blog/${blogPath}`;
                      },
                    blogSidebarCount: 'ALL',
                    postsPerPage: 'ALL',
                },
                theme: {
                    customCss: require.resolve('./src/css/custom.scss'),
                },
                sitemap: {
                    changefreq: 'daily',
                    priority: 0.5,
                },
                gtag: {
                    trackingID: 'G-WBQPTTG4ZG',
                    anonymizeIP: true,
                },
            }),
        ],
    ],
    plugins: [
        'docusaurus-plugin-sass',
        './src/plugins/pxToVw',
        './src/plugins/globalSassVarInject',
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'dev',
                path: '../docs/dev',
                routeBasePath: 'dev',
                sidebarPath: require.resolve('../docs/dev/sidebars.js'),
                editUrl: ({locale, devPath}) => {
                    if (locale !== config.i18n.defaultLocale) {
                      return `https://databend.crowdin.com/databend/${locale}`;
                    }
                    return `https://github.com/datafuselabs/databend/edit/main/docs/dev/${devPath}`;
                  },
            },
        ],
    ],
    themeConfig:
        /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
        ({
            announcementBar: {
                id: 'announcementBar-2', // Increment on change
                content: `⭐️ If you like Databend, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/datafuselabs/databend">GitHub</a> and follow us on <a target="_blank" rel="noopener noreferrer" href="https://twitter.com/datafuse_labs" >Twitter</a> ${TwitterSvg}`,
            },
            navbar: {
                title: 'Databend',
                logo: {
                    alt: 'Databend Logo',
                    src: 'img/logo/logo-no-text.svg',
                },
                items: [
                    {
                        to: '/doc',
                        label: 'Documentation',
                        position: 'right',
                    },
                    {
                        to: '/download',
                        label: 'Download',
                        position: 'right',
                    },
                    { to: '/blog', label: 'Blog', position: 'right' }, // or position: 'right'
                    {
                        href: 'https://github.com/datafuselabs/databend',
                        label: 'GitHub',
                        position: 'right',
                    },
                    {
                        type: 'localeDropdown',
                        position: 'right',
                        dropdownItemsAfter: [
                            {
                                to: 'https://databend.crowdin.com/databend',
                                label: 'Help Us Translate',
                            },
                        ],
                    },
                ],
            },
            footer: {
                style: 'dark',
                links: [
                    {
                        title: 'GET STARTED',
                        items: [
                            {
                                label: 'What is Databend',
                                to: '/doc'
                            },
                            {
                                label: 'Quick Start',
                                to: '/doc/guides'
                            },
                        ]
                    },
                    {
                        title: 'RESOURCES',
                        items: [
                            {
                                label: 'Deployment',
                                to: '/doc/deploy'
                            },
                            {
                                label: 'Releases',
                                to: '/doc/releases'
                            },
                        ]
                    },
                    {
                        title: 'COMMUNITY',
                        items: [
                            {
                                label: 'Slack',
                                href: 'https://link.databend.rs/join-slack',
                            },
                            {
                                label: 'Twitter',
                                href: 'https://twitter.com/Datafuse_Labs',
                            },
                        ],
                    },
                ],
                copyright: `Copyright © 2023 Datafuse Labs, Inc. Built with Docusaurus. <br><br> <img src="https://www.datocms-assets.com/31049/1618983297-powered-by-vercel.svg">`,
            },
            prism: {
                theme: lightCodeTheme,
                darkTheme: darkCodeTheme,
                additionalLanguages: ['toml', 'rust'],
            },
            algolia: {
                appId: 'RL7MS9PKE8',
                apiKey: 'cb5d6af612410c0fced698ff39ccd47a',
                indexName: 'databend-rs-docs',
                contextualSearch: true,
            },
            image: 'img/logo/logo-no-text.png',
            metadata: [
                { name: 'twitter:card', content: 'summary_large_image' },
                { name: 'twitter:site', content: '@databend.rs' }
            ],
        }),
};

module.exports = config;
