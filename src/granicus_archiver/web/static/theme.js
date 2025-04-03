(() => {
  const darkModeDropdown = document.getElementById('dark_mode_dropdown');
  const modeSelectButtons = darkModeDropdown.querySelectorAll('.theme-select-button');
  const darkModeMenuButton = document.getElementById('dark_mode_dropdown_button');

  const getPreferredColorScheme = (ignoreLocalStorage) => {
    if (!ignoreLocalStorage) {
      if (localStorage.getItem('coreui.theme')) {
        return localStorage.getItem('coreui.theme');
      }
    }
    const darkModeMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    return darkModeMediaQuery.matches ? 'dark' : 'light';
  };

  const setTheme = (theme) => {
    const isAuto = theme === 'auto';
    const selectedIcon = darkModeDropdown.querySelector(`.theme-select-button[data-theme-value="${theme}"] i`);
    const headerIcon = darkModeMenuButton.querySelector('i');
    headerIcon.className = selectedIcon.className;
    const realTheme = isAuto ? getPreferredColorScheme(true) : theme;

    document.documentElement.setAttribute('data-coreui-theme', realTheme);
    document.documentElement.setAttribute('data-bs-theme', realTheme);
    localStorage.setItem('coreui.theme', theme);
  };

  setTheme(getPreferredColorScheme(false));

  modeSelectButtons.forEach((button) => {
    button.addEventListener('click', (event) => {
      const selectedTheme = event.currentTarget.dataset.themeValue;
      setTheme(selectedTheme);
    });
  });
})();
